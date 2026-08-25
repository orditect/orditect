"""v0.3.0 批次 4 钉扎：R9 资源账 + 谱系豁免（递归组合反自锁）。

验收场景：
- 默认 task_execution 同名嵌套（原必炸场景）→ 豁免，不排队自锁
- 不同名嵌套 → 正常双 acquire 双 release
- 豁免路径 finally 不误释放祖先名额
- 无谱系/查询失败 → 安全默认正常 acquire
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.flow.exceptions import TaskNotFoundError


class FakeStorage:
    """支持谱系 + 资源账字段的内存存储。"""

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

    async def list_tasks(self, status=None, limit=100, offset=0):
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        return [dict(t) for t in tasks[offset:offset + limit]]


class CountingGovernor:
    """容量可配的计数 governor（默认 task_execution 容量 1——嵌套即排队）。"""

    def __init__(self, capacity: int = 1):
        self.capacity = capacity
        self.in_use = 0
        self.acquired: List[str] = []
        self.released: List[str] = []

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        if self.in_use >= self.capacity:
            from orditect.flow.exceptions import AcquireTimeoutError
            raise AcquireTimeoutError(f"resource full: {resource}")
        self.in_use += 1
        self.acquired.append(resource)
        return f"token-{len(self.acquired)}"

    async def try_acquire(self, resource):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.in_use = max(0, self.in_use - 1)
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return self.in_use


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class TestSameResourceExemption:
    """R9 核心验收：同名资源嵌套豁免（原"嵌套必炸"场景）。"""

    async def test_nested_same_resource_no_self_lock(self):
        """capacity=1 时父持 task_execution，子嵌套同名 → 豁免，不排队自锁。"""
        storage = FakeStorage()
        governor = CountingGovernor(capacity=1)  # 容量 1：不豁免则子必排队超时
        orchestrator = TaskOrchestrator(storage, governor)
        orchestrator.executor.acquire_timeout = 0.3  # 排队超时缩短，快速暴露自锁
        child_done = asyncio.Event()

        class ChildTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                child_done.set()
                return {"child": True}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                # nested submit (auto lineage + default same resource task_execution)
                child_id = await orchestrator.submit(ChildTask(storage))
                # wait child complete (exempt then smooth; not exempt then child queue timeout error)
                assert await _wait_for(
                    lambda: storage._tasks.get(child_id, {}).get("status") == "succeeded",
                    timeout=2.0,
                )
                return {"parent": True}

        parent_id = await orchestrator.submit(ParentTask(storage))

        # parent should succeed (child exempted, no self-lock)
        assert await _wait_for(
            lambda: storage._tasks.get(parent_id, {}).get("status") == "succeeded",
            timeout=3.0,
        )

        # acquire only once (parent), child exempt
        assert governor.acquired == ["task_execution"]
        # release only once (parent), child not wrongly released
        assert governor.released == ["task_execution"]
        assert governor.in_use == 0

    async def test_three_level_nesting_single_acquire(self):
        """三层同名嵌套 → 仅顶层 acquire 一次（整棵子树豁免）。"""
        storage = FakeStorage()
        governor = CountingGovernor(capacity=1)
        orchestrator = TaskOrchestrator(storage, governor)

        class L1(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"l1": True}

        class L2(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                cid = await orchestrator.submit(L1(storage))
                assert await _wait_for(
                    lambda: storage._tasks.get(cid, {}).get("status") == "succeeded"
                )
                return {"l2": True}

        class L3(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                cid = await orchestrator.submit(L2(storage))
                assert await _wait_for(
                    lambda: storage._tasks.get(cid, {}).get("status") == "succeeded"
                )
                return {"l3": True}

        root_id = await orchestrator.submit(L3(storage))
        assert await _wait_for(
            lambda: storage._tasks.get(root_id, {}).get("status") == "succeeded",
            timeout=3.0,
        )

        assert governor.acquired == ["task_execution"]  # 全程一次
        assert governor.released == ["task_execution"]


class TestDifferentResourceNormal:
    """不同名资源嵌套 → 正常双 acquire。"""

    async def test_different_resource_both_acquire(self):
        storage = FakeStorage()
        governor = CountingGovernor(capacity=10)
        orchestrator = TaskOrchestrator(storage, governor)

        class LLMTask(BaseBackEndTask):
            resource_type = "llm_pool"

            async def execute(self, task_id: str, **kwargs):
                return {"llm": True}

        class ParentTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                cid = await orchestrator.submit(LLMTask(storage))
                assert await _wait_for(
                    lambda: storage._tasks.get(cid, {}).get("status") == "succeeded"
                )
                return {"parent": True}

        parent_id = await orchestrator.submit(ParentTask(storage))
        assert await _wait_for(
            lambda: storage._tasks.get(parent_id, {}).get("status") == "succeeded"
        )

        # two different resources each acquire/release once
        assert set(governor.acquired) == {"task_execution", "llm_pool"}
        assert set(governor.released) == {"task_execution", "llm_pool"}


class TestExemptionSafetyDefaults:
    """豁免的安全默认行为。"""

    async def test_no_lineage_normal_acquire(self):
        """顶层任务（无谱系）→ 正常 acquire。"""
        storage = FakeStorage()
        governor = CountingGovernor(capacity=1)
        orchestrator = TaskOrchestrator(storage, governor)

        class SimpleTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"ok": True}

        task_id = await orchestrator.submit(SimpleTask(storage))
        assert await _wait_for(
            lambda: storage._tasks.get(task_id, {}).get("status") == "succeeded"
        )
        assert governor.acquired == ["task_execution"]

    async def test_lineage_query_failure_falls_back_to_acquire(self):
        """谱系查询失败 → 安全默认正常 acquire（不误豁免）。"""
        class BrokenLineageStorage(FakeStorage):
            async def get_task(self, task_id):
                # error when querying parent
                if "child" in task_id:
                    raise RuntimeError("storage hiccup")
                return await super().get_task(task_id)

        storage = BrokenLineageStorage()
        governor = CountingGovernor(capacity=10)
        orchestrator = TaskOrchestrator(storage, governor)

        # manually construct lineage (bypass submit auto injection)
        await storage.initialize_task("parent", "running")
        await storage.initialize_task("child", "queued", parent_task_id="parent")

        # directly execute child task (get_task(child) will throw error → safe default)
        class SimpleTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"ok": True}

        # note: executor's _is_cancel_requested also fails (returns False), continue execution
        result = await orchestrator.executor.execute("child", SimpleTask(storage))
        assert result == {"ok": True}
        assert "task_execution" in governor.acquired  # 正常 acquire（未误豁免）

    async def test_ancestor_without_resource_continues_upward(self):
        """中间祖先未持资源 → 继续上溯到持资源的祖先。"""
        storage = FakeStorage()
        governor = CountingGovernor(capacity=1)
        orchestrator = TaskOrchestrator(storage, governor)

        # manual lineage: root(holds task_execution) → mid(no resource field) → leaf
        await storage.initialize_task("root", "running")
        await storage.update_task("root", {"resource": "task_execution"})
        await storage.initialize_task("mid", "running", parent_task_id="root")
        # mid has no resource field (simulate intermediate layer with governor=None)
        await storage.initialize_task("leaf", "queued", parent_task_id="mid")

        class SimpleTask(BaseBackEndTask):
            async def execute(self, task_id: str, **kwargs):
                return {"ok": True}

        # leaf execution: trace up mid (no resource) → root (has task_execution) → same-name exempt
        result = await orchestrator.executor.execute("leaf", SimpleTask(storage))
        assert result == {"ok": True}
        assert governor.acquired == []  # 豁免，未新 acquire