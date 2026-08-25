"""v0.3.2 批次 2 钉扎：预算幂等键双栖 + cancel 竞态幂等 + 查询效率。

真实 Redis 用例（预算建账/双栖去重）标记 integration；
竞态/查询计数/退避用例为纯内存（FakeStorage）。
"""
import asyncio
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import (
    BudgetExhaustedError,
    BudgetLedger,
    GovernedClient,
    TaskExecutor,
    TaskLifecycle,
    TaskOrchestrator,
    TaskStateMachine,
)
from orditect.flow.exceptions import TaskNotFoundError


# ---------- test infrastructure ----------

class FakeStorage:
    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(
        self, task_id: str, initial_status: str, *,
        parent_task_id: Optional[str] = None, if_not_exists: bool = False,
    ) -> bool:
        if if_not_exists and task_id in self._tasks:
            return False
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "progress": 0.0, "cancel_requested": False,
        }
        if parent_task_id is not None:
            self._tasks[task_id]["parent_task_id"] = parent_task_id
        return True

    async def list_children(self, parent_task_id: str) -> List[str]:
        return [tid for tid, t in self._tasks.items()
                if t.get("parent_task_id") == parent_task_id]

    async def update_task(self, task_id, updates, **kwargs):
        if task_id not in self._tasks:
            raise TaskNotFoundError(task_id)
        self._tasks[task_id].update(updates)

    async def get_task(self, task_id):
        if task_id not in self._tasks:
            raise TaskNotFoundError(task_id)
        return dict(self._tasks[task_id])

    async def request_cancel(self, task_id):
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True


class SpyGovernor:
    def __init__(self):
        self.acquired: List[str] = []
        self.released: List[str] = []

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        return f"spy-{len(self.acquired)}"

    async def try_acquire(self, resource):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


# ---------- budget cluster (needs real taskbase quota) ----------

@pytest.mark.integration
class TestBudgetV032:
    async def test_open_registers_ledger_lease(self, redis_client):
        """open() 真建账（v0.3.1 静默失败：units=0 被 Lua invalid_units 拒绝）。"""
        from orditect.core import AdmissionQuotaRedisDB
        quota = AdmissionQuotaRedisDB(client=redis_client)
        await quota.connect()

        ledger = BudgetLedger(quota, root_task_id="root-1", max_units=100)
        await ledger.open()

        # lease position registered (score exists), no quota consumed
        score = await redis_client.zscore("admission:budget:root-1:leases", "__ledger__")
        assert score is not None
        assert await ledger.balance() == 100

    async def test_stable_call_id_charges_once(self, redis_client):
        """call_id 双栖键：同一 call_id 重试只扣一次（quota 热路径去重）。"""
        from orditect.core import AdmissionQuotaRedisDB
        quota = AdmissionQuotaRedisDB(client=redis_client)
        await quota.connect()

        ledger = BudgetLedger(quota, root_task_id="root-2", max_units=10)
        await ledger.open()

        async def handler():
            return "ok"

        client = GovernedClient(SpyGovernor(), "res", handler=handler, budget=ledger)

        await client.call(call_id="c-1")
        await client.call(call_id="c-1")  # 业务重试，同一幂等键

        assert await ledger.balance() == 9  # 只扣一次

    async def test_exhausted_budget_blocks_before_acquire(self, redis_client):
        """#23：预算耗尽时预检在 acquire 之前拦截（不排队占资源）。"""
        from orditect.core import AdmissionQuotaRedisDB
        quota = AdmissionQuotaRedisDB(client=redis_client)
        await quota.connect()

        ledger = BudgetLedger(quota, root_task_id="root-3", max_units=1)
        await ledger.open()
        await ledger.charge(1, call_id="seed")  # 预耗尽

        governor = SpyGovernor()

        async def handler():
            return "ok"

        client = GovernedClient(governor, "res", handler=handler, budget=ledger)

        with pytest.raises(BudgetExhaustedError):
            await client.call()

        assert governor.acquired == []  # 未 acquire（v0.3.1 会先 acquire 再拦截）


# ---------- #13: cancel race idempotent cleanup ----------

class _FlipStorage(FakeStorage):
    """模拟 executor 1c 竞态闭环：首次读 running 后，底层记录翻转为 cancelled。"""

    def __init__(self):
        super().__init__()
        self._armed = True

    async def get_task(self, task_id):
        rec = await super().get_task(task_id)
        if self._armed and rec.get("status") == "running":
            self._armed = False
            # underlying flipped to cancelled (executor 1c closed first), this returns running
            self._tasks[task_id]["status"] = "cancelled"
            self._tasks[task_id]["cancel_requested"] = True
            rec = dict(rec)
            rec["status"] = "running"
        return rec


class TestCancelRaceIdempotent:
    async def test_cancel_race_with_executor_settle_returns_true(self):
        """#13：transition 竞态撞终态 → 幂等确认为成功（v0.3.1 抛 InvalidStateTransitionError）。"""
        storage = _FlipStorage()
        lifecycle = TaskLifecycle(storage, TaskStateMachine())

        await storage.initialize_task("t", "running")

        ok = await lifecycle.cancel("t")
        assert ok is True
        assert (await storage.get_task("t"))["status"] == "cancelled"

    async def test_cancel_after_succeeded_returns_false(self):
        """竞态输给 SUCCEEDED：返回 False，状态不被覆盖（回归）。"""
        storage = FakeStorage()
        lifecycle = TaskLifecycle(storage, TaskStateMachine())

        await storage.initialize_task("t", "succeeded")

        ok = await lifecycle.cancel("t")
        assert ok is False
        assert (await storage.get_task("t"))["status"] == "succeeded"


# ---------- #19: lineage query deduplication ----------

class _CountingStorage(FakeStorage):
    def __init__(self):
        super().__init__()
        self.get_task_calls = 0

    async def get_task(self, task_id):
        self.get_task_calls += 1
        return await super().get_task(task_id)


class TestAncestorQueryDedup:
    async def test_ancestor_query_count(self):
        """#19 回归 + F2 集合化：root(持资源) ← mid ← leaf，查询恰好 3 次。

        v0.3.3：方法改名 _find_ancestor_resources（返回值 str|None → set[str]），
        查询次数不变（集合化不增加查询）。
        """
        storage = _CountingStorage()
        await storage.initialize_task("root", "running")
        await storage.update_task("root", {"resource": "task_execution"})
        await storage.initialize_task("mid", "running", parent_task_id="root")
        await storage.initialize_task("leaf", "queued", parent_task_id="mid")

        executor = TaskExecutor(storage, governor=None)
        storage.get_task_calls = 0
        found = await executor._find_ancestor_resources("leaf")  # 改：复数方法名

        assert found == {"task_execution"}  # 改：集合断言
        assert storage.get_task_calls == 3

# ---------- #20: wait_terminal backoff ----------

class TestWaitTerminalBackoff:
    async def test_long_task_polls_with_backoff(self):
        """#20：0.5s 超时窗口内轮询次数显著少于固定 50ms（~10 次）。"""
        storage = _CountingStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)
        await storage.initialize_task("t", "running")

        storage.get_task_calls = 0
        with pytest.raises(TimeoutError):
            await orchestrator.wait_terminal("t", timeout=0.5, poll_interval=0.05)

        # backoff sequence 0.05/0.075/0.1125/0.169/0.253 → about 5 polls;
        # fixed 50ms → about 10 polls. Threshold 7 prevents timing jitter.
        assert storage.get_task_calls <= 7

    async def test_quick_terminal_unaffected(self):
        """回归：快速终态任务仍立即返回（退避不影响短任务）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)
        await storage.initialize_task("t", "succeeded")

        record = await orchestrator.wait_terminal("t", timeout=1.0)
        assert record["status"] == "succeeded"