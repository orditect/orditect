"""executor v0.3.0 手术钉扎测试（1c / R5 / R17 / R12 / #2）。

纯内存基建（FakeStorage / SpyGovernor），无需 Redis。
"""
import asyncio
import time
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import BaseBackEndTask, TaskExecutor
from orditect.flow.exceptions import TaskNotFoundError


# ---------- test infrastructure ----------

class FakeStorage:
    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(self, task_id: str, initial_status: str) -> None:
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "progress": 0.0, "cancel_requested": False,
        }

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

    async def list_tasks(self, status=None, limit=100, offset=0):
        tasks = list(self._tasks.values())
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        return [dict(t) for t in tasks[offset:offset + limit]]


class SpyGovernor:
    def __init__(self, acquire_delay: float = 0.0, release_delay: float = 0.0):
        self.acquired: List[str] = []
        self.released: List[str] = []
        self._counter = 0
        self._acquire_delay = acquire_delay
        self._release_delay = release_delay

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        if self._acquire_delay > 0:
            if timeout is not None and self._acquire_delay > timeout:
                await asyncio.sleep(timeout)
                from orditect.flow.exceptions import AcquireTimeoutError
                raise AcquireTimeoutError(f"acquire timeout: {resource}")
            await asyncio.sleep(self._acquire_delay)
        self.acquired.append(resource)
        self._counter += 1
        return f"spy-token-{self._counter}"

    async def try_acquire(self, resource: str) -> Optional[str]:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        if self._release_delay > 0:
            await asyncio.sleep(self._release_delay)
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return len(self.acquired) - len(self.released)


class RecordingTask(BaseBackEndTask):
    """记录钩子调用的任务。"""

    def __init__(self, storage, governor=None, sleep: float = 0.05,
                 fail: Exception | None = None):
        super().__init__(storage, governor)
        self._sleep = sleep
        self._fail = fail
        self.hooks: List[str] = []

    async def execute(self, task_id: str, **kwargs):
        if self._sleep > 0:
            await asyncio.sleep(self._sleep)
        if self._fail is not None:
            raise self._fail
        return {"done": True}

    async def on_success(self, task_id: str, result: Any) -> None:
        self.hooks.append("on_success")

    async def on_failure(self, task_id: str, error: Exception) -> None:
        self.hooks.append("on_failure")

    async def on_cancel(self, task_id: str) -> None:
        self.hooks.append("on_cancel")


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


# ---------- 1c: task with cancel flag finishes as CANCELLED ----------

class Test1cCancelledSettle:
    async def test_cancel_then_success_settles_cancelled(self):
        """任务成功但已被标记取消 → 状态 cancelled + on_cancel（修复前：succeeded + on_success）。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy, sleep=0.15)

        runner = asyncio.create_task(executor.execute(task_id="t1", task=task))
        assert await _wait_for(lambda: executor.is_running("t1"))

        # mark cancel during task run (no kill coroutine)
        await executor.cancel("t1", force=False)

        result = await runner  # 跑完
        assert result == {"done": True}

        stored = await storage.get_task("t1")
        assert stored["status"] == "cancelled"
        assert stored["cancel_outcome"] == "succeeded_but_cancelled"
        assert task.hooks == ["on_cancel"]  # 不是 on_success
        assert spy.released == ["task_execution"]  # sem 正常释放

    async def test_cancel_then_failure_settles_cancelled(self):
        """任务失败但已被标记取消 → 状态 cancelled + on_cancel（修复前：failed + on_failure）。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy, sleep=0.15, fail=ValueError("boom"))

        runner = asyncio.create_task(executor.execute(task_id="t1", task=task))
        assert await _wait_for(lambda: executor.is_running("t1"))
        await executor.cancel("t1", force=False)

        with pytest.raises(ValueError, match="boom"):
            await runner

        stored = await storage.get_task("t1")
        assert stored["status"] == "cancelled"
        assert stored["cancel_outcome"] == "failed_but_cancelled"
        assert task.hooks == ["on_cancel"]  # 不是 on_failure

    async def test_no_cancel_success_normal(self):
        """未取消时正常成功（回归）。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, sleep=0.01)

        await executor.execute(task_id="t1", task=task)

        stored = await storage.get_task("t1")
        assert stored["status"] == "succeeded"
        assert task.hooks == ["on_success"]


# ---------- R5: TOCTOU pre-check ----------

class TestR5ToctouGuard:
    async def test_pre_cancelled_task_skips_acquire(self):
        """预置 cancel 标记后 execute：不 acquire、不执行、直接走 CancelledError 闭环。"""
        storage = FakeStorage()
        spy = SpyGovernor()
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        await storage.request_cancel("t1")  # 预置标记（模拟 terminate/submit 竞态）

        task = RecordingTask(storage, spy)
        with pytest.raises(asyncio.CancelledError):
            await executor.execute(task_id="t1", task=task)

        assert spy.acquired == []  # 未获取资源（协程不复活）
        stored = await storage.get_task("t1")
        assert stored["status"] == "cancelled"  # CancelledError 分支落终态
        assert task.hooks == ["on_cancel"]


# ---------- R17: distinguish business TimeoutError from execution timeout ----------

class TestR17TimeoutDistinction:
    async def test_business_timeout_error_is_failure_not_execution_timeout(self):
        """业务代码抛内置 TimeoutError → 走普通失败分支，error 含业务消息。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, sleep=0.01, fail=TimeoutError("downstream LLM timeout"))

        with pytest.raises(TimeoutError, match="downstream LLM timeout"):
            await executor.execute(task_id="t1", task=task, timeout=5.0)

        stored = await storage.get_task("t1")
        assert stored["status"] == "failed"
        assert "downstream LLM timeout" in stored["error"]
        assert stored["error"] != "Task execution timeout"  # 不误标

    async def test_real_execution_timeout(self):
        """真正的执行超时（wait 到期）→ error 为执行超时。"""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, sleep=5.0)  # 远超 timeout

        runner = asyncio.create_task(
            executor.execute(task_id="t1", task=task, timeout=0.1)
        )

        with pytest.raises(asyncio.TimeoutError):
            await runner

        # wait execute coroutine fully end (shield cleanup complete, coroutine exited) —
        # when await runner gets exception, coroutine may still be in shield cleanup
        assert await _wait_for(lambda: runner.done(), timeout=2.0)
        assert await _wait_for(lambda: not executor.is_running("t1"), timeout=2.0)
        # wait shield cleanup tasks all complete (prevent event loop teardown create_task hit closed loop)
        assert await _wait_for(lambda: len(executor._finalize_tasks) == 0, timeout=2.0)

        stored = await storage.get_task("t1")
        assert stored["status"] == "failed"
        assert "Task execution timeout" in stored["error"]
# ---------- R12: second cancel does not swallow release ----------

class TestR12ShieldedRelease:
    async def test_double_cancel_release_completes(self):
        """release 较慢 + 二次取消打在 finally 上 → release 仍完成（shield）。"""
        storage = FakeStorage()
        # release needs 0.3s (slow)
        spy = SpyGovernor(release_delay=0.3)
        executor = TaskExecutor(storage, spy)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy, sleep=0.05)

        runner = asyncio.create_task(executor.execute(task_id="t1", task=task))
        assert await _wait_for(lambda: executor.is_running("t1"))

        # wait task finish entering finally's release (slow release in progress)
        await asyncio.sleep(0.15)

        # second cancel: hits shield, release should continue
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass

        # release inside shield is independent task, wait it complete
        assert await _wait_for(lambda: spy.released == ["task_execution"], timeout=2.0)
        # v0.3.3: wait runner coroutine and finalize task all finish, prevent teardown GC force kill
        assert await _wait_for(lambda: runner.done(), timeout=2.0)
        assert await _wait_for(lambda: len(executor._finalize_tasks) == 0, timeout=2.0)

# ---------- #2: acquire_timeout parameterized ----------

class TestAcquireTimeoutParam:
    async def test_acquire_timeout_enforced(self):
        """acquire_timeout=0.1 且 acquire 需要 0.5s → 排队超时。"""
        storage = FakeStorage()
        spy = SpyGovernor(acquire_delay=0.5)
        executor = TaskExecutor(storage, spy, acquire_timeout=0.1)

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy)

        from orditect.flow.exceptions import AcquireTimeoutError
        with pytest.raises(AcquireTimeoutError):
            await executor.execute(task_id="t1", task=task)

        assert spy.acquired == []  # 排队超时，未获取到

    async def test_acquire_timeout_none_waits_forever(self):
        """默认（None）：无限排队，慢 acquire 最终成功。"""
        storage = FakeStorage()
        spy = SpyGovernor(acquire_delay=0.1)
        executor = TaskExecutor(storage, spy)  # acquire_timeout=None

        await storage.initialize_task("t1", "pending")
        task = RecordingTask(storage, spy)

        result = await executor.execute(task_id="t1", task=task)
        assert result == {"done": True}
        assert spy.acquired == ["task_execution"]