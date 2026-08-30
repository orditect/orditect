"""F3 pinning: result reuse short-circuit (option B — hot-record result).

- Default (no query / no reuse words): never short-circuits (zero behavior change).
- Latest generation succeeded + hot-record result present -> reuse, no re-execution.
- No snapshot / no result -> normal execution.
- Query failure -> explicit fallback to normal execution (T8, no silent misjudge).
"""
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import BaseBackEndTask, TaskExecutor
from orditect.flow.exceptions import TaskNotFoundError


class FakeStorage:
    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(self, task_id: str, initial_status: str, **kw: Any) -> bool:
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "execution_id": f"exec-{task_id}",
            "progress": 0.0, "cancel_requested": False,
        }
        return True

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


class StubQuery:
    """Programmable reuse query."""

    def __init__(self, status: Optional[str] = None, fail: bool = False):
        self._status = status
        self._fail = fail

    async def latest_status(self, task_id: str, step: str = "execute") -> Optional[str]:
        if self._fail:
            raise RuntimeError("snapshot store down")
        return self._status


class CountingTask(BaseBackEndTask):
    """Counts execute invocations to detect re-execution."""

    def __init__(self, storage, governor=None):
        super().__init__(storage, governor)
        self.exec_count = 0

    async def execute(self, task_id: str, **kwargs):
        self.exec_count += 1
        return {"computed": self.exec_count}


@pytest.mark.unit
class TestResultReuse:
    async def test_reuse_skips_reexecution(self):
        """Latest=success + hot-record result -> reuse, execute NOT called."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")
        await storage.update_task("t1", {"result": {"cached": True}})  # prior result

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(status="succeeded"),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t1", task=task)

        assert result == {"cached": True}   # reused prior result
        assert task.exec_count == 0          # execute NOT called

    async def test_no_reuse_word_no_short_circuit(self):
        """reuse_terminal_words empty -> never short-circuits (even with query)."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")
        await storage.update_task("t1", {"result": {"cached": True}})

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(status="succeeded"),
            reuse_terminal_words=None,  # no words -> never reuse
        )
        result = await executor.execute(task_id="t1", task=task)

        assert result == {"computed": 1}
        assert task.exec_count == 1

    async def test_no_snapshot_normal_execution(self):
        """No snapshot (latest=None) -> normal execution."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(status=None),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t1", task=task)
        assert result == {"computed": 1}

    async def test_query_failure_falls_back(self):
        """T8: snapshot store down -> explicit fallback to normal execution."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(fail=True),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t1", task=task)
        assert result == {"computed": 1}  # fell back to normal execution

    async def test_default_no_reuse_configured(self):
        """Default executor (no snapshot_query/reuse words) -> never reuses."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")
        await storage.update_task("t1", {"result": {"cached": True}})

        task = CountingTask(storage)
        executor = TaskExecutor(storage, governor=None)  # no F3 config
        result = await executor.execute(task_id="t1", task=task)
        assert result == {"computed": 1}
        assert task.exec_count == 1

class SpyGovernor:
    """Records acquire/release calls (proves the reuse path never touches
    the semaphore)."""

    def __init__(self):
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        return f"spy-token-{len(self.acquired)}"

    async def try_acquire(self, resource: str):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


@pytest.mark.unit
class TestResultReuseOrdering:
    """v0.1.6 pinning: the F3 reuse check runs BEFORE acquire and BEFORE the
    running write.

    Red before: with the check placed after the running write, a reused node
    (a) acquired a semaphore slot it never needed, and (b) left the hot
    record stuck at RUNNING forever (wait_terminal would time out).
    """

    async def test_reuse_never_acquires_governor(self):
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")
        await storage.update_task("t1", {"result": {"cached": True}})

        governor = SpyGovernor()
        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=governor,
            snapshot_query=StubQuery(status="succeeded"),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t1", task=task)

        assert result == {"cached": True}
        assert task.exec_count == 0
        assert governor.acquired == []
        assert governor.released == []

    async def test_reuse_does_not_write_running_status(self):
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")
        await storage.update_task("t1", {"result": {"cached": True}})

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(status="succeeded"),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        await executor.execute(task_id="t1", task=task)

        stored = await storage.get_task("t1")
        # The reuse path must not leave the hot record stuck at running.
        assert stored["status"] == "pending"

    async def test_non_reused_node_still_acquires_and_settles(self):
        """Regression: a node that does NOT hit reuse still acquires and
        settles normally (the order change must not disturb the main path)."""
        storage = FakeStorage()
        await storage.initialize_task("t1", "pending")

        governor = SpyGovernor()
        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=governor,
            snapshot_query=StubQuery(status=None),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t1", task=task)

        assert result == {"computed": 1}
        assert governor.acquired == ["task_execution"]
        assert governor.released == ["task_execution"]
        stored = await storage.get_task("t1")
        assert stored["status"] == "succeeded"

@pytest.mark.unit
class TestNoneResultReuse:
    """v0.1.7 pinning (adjudicated #11): executor and recovery agree on
    None-result reuse (key presence, not non-None) for side-effect tasks.

    Red before: RecoveryService.decide used `rec.get("result") is not None`,
    so a side-effect task (returns None) was reused by the executor but
    rerun forever by recovery — the two rules disagreed.
    """

    async def test_executor_reuses_none_result(self):
        storage = FakeStorage()
        await storage.initialize_task("t_none", "pending")
        await storage.update_task("t_none", {"result": None})  # side-effect task done

        task = CountingTask(storage)
        executor = TaskExecutor(
            storage, governor=None,
            snapshot_query=StubQuery(status="succeeded"),
            reuse_terminal_words=frozenset({"succeeded"}),
        )
        result = await executor.execute(task_id="t_none", task=task)
        assert result is None
        assert task.exec_count == 0  # reused, not re-executed

    async def test_recovery_reuses_none_result(self):
        from orditect.flow.recovery import RecoveryService, ReuseDecision

        storage = FakeStorage()
        await storage.initialize_task("t_none", "pending")
        await storage.update_task("t_none", {"result": None})

        class _Reader:
            async def get(self, task_id, step="execute"):
                class S:
                    status = "succeeded"
                return S()

            async def get_tree(self, root, **kw):
                return []

        svc = RecoveryService(
            storage, _Reader(), executor=None,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=None,
        ) if False else None
        # decide() is the adjudicated seam; drive it directly without a
        # full RecoveryService construction (executor/factory not needed
        # for a REUSE decision).
        from orditect.flow.recovery.service import RecoveryService as _RS
        svc = _RS.__new__(_RS)
        svc._storage = storage
        svc._reader = _Reader()
        svc._reuse_words = frozenset({"succeeded"})

        decision = await svc.decide("t_none")
        assert decision is ReuseDecision.REUSE