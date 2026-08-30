"""cancel 增强单元测试（升级清单任务 3：行业通行两种取消模式）。

覆盖：
- TaskExecutor.cancel(force=False)：优雅模式，仅标记 CancellationToken
- TaskExecutor.cancel(force=True)：强制模式，取消运行中的 asyncio 协程，
  状态流转 CANCELLED、调用 on_cancel 钩子、资源令牌由 finally 释放
- TaskOrchestrator.terminate()：
  a) 协程在本进程运行中 → 取消协程，execute() 的 CancelledError 分支闭环
  b) 协程不在本进程（queued 未启动）→ 兜底直接流转 CANCELLED
  c) 终态任务 → False
  d) 任务不存在 → False
- orchestrator.cancel() 优雅标记（既有路径回归）

测试基建：内存 FakeStorage / SpyGovernor，纯内存测试，无需 Redis。
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import (
    BaseBackEndTask,
    TaskExecutor,
    TaskOrchestrator,
)
from orditect.flow.exceptions import TaskNotFoundError


# ---------- test infrastructure ----------

class FakeStorage:
    """内存任务存储（实现 TaskStorageProtocol）。

    update_task 带 **kwargs：executor/lifecycle 调用时会传
    validate_status_transfer=False，协议未声明该参数，
    这里用 **kwargs 兼容（与 taskbase TaskRedisDB 的实际签名对齐）。
    """

    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(
        self,
        task_id: str,
        initial_status: str,
        *,
        parent_task_id: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> bool:
        if if_not_exists and task_id in self._tasks:
            return False
        self._tasks[task_id] = {
            "task_id": task_id,
            "status": initial_status,
            "progress": 0.0,
            "cancel_requested": False,
        }
        if parent_task_id is not None:
            self._tasks[task_id]["parent_task_id"] = parent_task_id
        return True

    async def list_children(self, parent_task_id: str) -> List[str]:
        return [
            tid for tid, t in self._tasks.items()
            if t.get("parent_task_id") == parent_task_id
        ]

    async def update_task(self, task_id: str, updates: Dict[str, Any], **kwargs) -> None:
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        self._tasks[task_id].update(updates)

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        if task_id not in self._tasks:
            raise TaskNotFoundError(f"task_id not found: {task_id}")
        return dict(self._tasks[task_id])

    async def request_cancel(self, task_id: str) -> bool:
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True

    async def list_tasks(
        self,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        return [dict(t) for t in tasks[offset:offset + limit]]


class SpyGovernor:
    """测试治理：记录 acquire/release 调用的资源名。"""

    def __init__(self):
        self.acquired: List[str] = []
        self.released: List[str] = []
        self._counter = 0

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        self._counter += 1
        return f"spy-token-{self._counter}"

    async def try_acquire(self, resource: str) -> Optional[str]:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


class LongRunningTask(BaseBackEndTask):
    """长任务：loop 睡眠，on_cancel 记录调用。"""

    def __init__(self, storage, governor=None, loop_count: int = 200):
        super().__init__(storage, governor)
        self.cancelled_hook_called = False
        self._loop_count = loop_count

    async def execute(self, task_id: str, **kwargs):
        for _ in range(self._loop_count):
            await asyncio.sleep(0.05)
        return {"done": True}

    async def on_cancel(self, task_id: str) -> None:
        self.cancelled_hook_called = True


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    """轮询等待条件满足。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


# ---------- TaskExecutor.cancel dual mode ----------

class TestExecutorCancel:
    """TaskExecutor.cancel(force) 双模式测试。"""

    async def test_graceful_cancel_marks_token_only(self):
        """force=False：仅标记 cancel_requested，不取消协程。

        v0.3.0（1c 修复后语义翻转）：
        任务继续跑完，但终态落 CANCELLED（不再是 SUCCEEDED），
        钩子调 on_cancel（不再是 on_success）。
        """
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        # loop_count=2: task finishes in 0.1s, verify graceful mark does not interrupt execution
        task = LongRunningTask(storage, spy, loop_count=2)

        runner = asyncio.create_task(executor.execute(task_id="t1", task=task))
        # wait coroutine registration
        assert await _wait_for(lambda: executor.is_running("t1"))

        await executor.cancel("t1", force=False)

        stored = await storage.get_task("t1")
        assert stored["cancel_requested"] is True
        assert executor.is_running("t1") is True  # 协程未被取消

        result = await runner  # 任务正常跑完
        assert result == {"done": True}

        # 1c fix: task marked cancelled finishes as CANCELLED, not SUCCEEDED
        stored = await storage.get_task("t1")
        assert stored["status"] == "cancelled"
        assert task.cancelled_hook_called is True  # on_cancel 而非 on_success

    async def test_graceful_cancel_nonexistent_task_no_raise(self):
        """force=False 对不存在任务：静默记录日志，不抛异常。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await executor.cancel("ghost", force=False)  # 不抛异常即通过

    async def test_force_cancel_terminates_coroutine(self):
        """force=True：协程被取消，状态 CANCELLED，on_cancel 调用，资源释放。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = LongRunningTask(storage, spy)

        runner = asyncio.create_task(executor.execute(task_id="t1", task=task))
        assert await _wait_for(lambda: executor.is_running("t1"))

        await executor.cancel("t1", force=True)

        with pytest.raises(asyncio.CancelledError):
            await runner

        stored = await storage.get_task("t1")
        assert stored["status"] == "cancelled"
        assert task.cancelled_hook_called is True
        assert spy.released == ["task_execution"]
        assert executor.is_running("t1") is False

        # v0.3.3: drain shield cleanup
        assert await _wait_for(lambda: len(executor._finalize_tasks) == 0, timeout=2.0)

    async def test_force_cancel_not_running_no_raise(self):
        """force=True 但协程不在本进程：静默记录日志，不抛异常。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await executor.cancel("t1", force=True)  # 不抛异常即通过


# ---------- TaskOrchestrator.terminate ----------

class TestOrchestratorTerminate:
    """TaskOrchestrator.terminate() 测试。"""

    async def test_terminate_running_task(self):
        """协程在本进程：强制终止，状态 CANCELLED，资源释放，返回 True。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)

        task = LongRunningTask(storage, spy)
        task_id = await orchestrator.submit(task)

        assert await _wait_for(lambda: orchestrator.is_running(task_id))
        assert await _wait_for(lambda: spy.acquired != [])

        ok = await orchestrator.terminate(task_id)
        assert ok is True

        assert await _wait_for(lambda: not orchestrator.is_running(task_id))
        stored = await storage.get_task(task_id)
        assert stored["status"] == "cancelled"
        assert stored["cancel_requested"] is True
        assert task.cancelled_hook_called is True
        assert spy.released == ["task_execution"]

        # v0.3.3: drain bg task and shield cleanup (prevent teardown GC force kill)
        await orchestrator.wait_all_finalized()

    async def test_terminate_queued_task_fallback(self):
        """协程不在本进程（预置 queued 记录）：兜底直接流转 CANCELLED。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)

        # preset queued task record (no submit, no coroutine)
        await storage.initialize_task("queued_task", "queued")

        ok = await orchestrator.terminate("queued_task")
        assert ok is True

        stored = await storage.get_task("queued_task")
        assert stored["status"] == "cancelled"
        assert stored["cancel_requested"] is True
        # no coroutine path no resource release involved
        assert spy.released == []

    async def test_terminate_terminal_task_returns_false(self):
        """终态任务：返回 False，状态不变。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("done_task", "succeeded")

        ok = await orchestrator.terminate("done_task")
        assert ok is False

        stored = await storage.get_task("done_task")
        assert stored["status"] == "succeeded"

    async def test_terminate_nonexistent_task_returns_false(self):
        """任务不存在：返回 False。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        ok = await orchestrator.terminate("ghost")
        assert ok is False

    async def test_terminate_after_submit_e2e(self):
        """端到端：submit → terminate → 终态 cancelled。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        orchestrator = TaskOrchestrator(storage, spy)

        task = LongRunningTask(storage, spy)
        task_id = await orchestrator.submit(task)

        ok = await orchestrator.terminate(task_id)
        assert ok is True

        assert await _wait_for(lambda: not orchestrator.is_running(task_id))
        status = await orchestrator.get_status(task_id)
        assert status.value == "cancelled"

        # v0.3.3: drain
        await orchestrator.wait_all_finalized()

    async def test_terminate_request_cancel_refused_returns_false(self):
        """v0.1.6 pinning: a request_cancel refusal (task vanished or already
        terminal between the reads) yields False instead of proceeding.

        Red before: the return value of request_cancel was ignored, so a
        refused cancel still flowed into the CANCELLED transition."""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t_refused", "running")

        async def refuse_cancel(task_id: str) -> bool:
            return False

        storage.request_cancel = refuse_cancel
        ok = await orchestrator.terminate("t_refused")
        assert ok is False

        stored = await storage.get_task("t_refused")
        assert stored["status"] == "running"
        assert stored["cancel_requested"] is False

    async def test_terminate_task_vanishes_midway_returns_false(self):
        """v0.1.6 pinning: a task deleted between get_task and request_cancel
        returns False instead of leaking TaskNotFoundError."""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t_vanish", "running")

        original_request = storage.request_cancel

        async def vanish_on_cancel(task_id: str) -> bool:
            del storage._tasks[task_id]
            return await original_request(task_id)

        storage.request_cancel = vanish_on_cancel
        ok = await orchestrator.terminate("t_vanish")
        assert ok is False

    async def test_terminate_task_vanishes_before_transition(self):
        """v0.1.6 pinning: a task deleted between request_cancel and the
        fallback transition returns False instead of leaking
        TaskNotFoundError."""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t_vanish2", "running")

        original_transition = orchestrator.lifecycle.transition_to

        async def vanish_on_transition(task_id: str, to_status) -> None:
            del storage._tasks[task_id]
            await original_transition(task_id, to_status)

        orchestrator.lifecycle.transition_to = vanish_on_transition
        ok = await orchestrator.terminate("t_vanish2")
        assert ok is False

    async def test_terminate_normal_path_still_works(self):
        """Regression: the honored request_cancel return must not break the
        normal fallback termination path."""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t_normal", "queued")
        ok = await orchestrator.terminate("t_normal")
        assert ok is True

        stored = await storage.get_task("t_normal")
        assert stored["status"] == "cancelled"
        assert stored["cancel_requested"] is True


# ---------- orchestrator.cancel graceful mode (existing path regression) ----------

class TestOrchestratorGracefulCancel:
    """orchestrator.cancel() 优雅标记（既有行为回归）。"""

    async def test_cancel_marks_and_transitions(self):
        """cancel() 标记 cancel_requested 并流转 CANCELLED（lifecycle 路径）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t1", "pending")

        ok = await orchestrator.cancel("t1")
        assert ok is True

        stored = await storage.get_task("t1")
        assert stored["cancel_requested"] is True
        assert stored["status"] == "cancelled"

    async def test_cancel_terminal_task_returns_false(self):
        """终态任务 cancel 返回 False（既有行为）。"""
        storage = FakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)

        await storage.initialize_task("t1", "succeeded")

        ok = await orchestrator.cancel("t1")
        assert ok is False