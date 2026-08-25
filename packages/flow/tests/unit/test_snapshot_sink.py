"""F2 pinning: snapshot sink injection point.

- Default NullSink: zero behavior change (existing 130 tests stay green).
- Injected sink receives writes at running / succeeded / failed / cancelled.
- Sink failure never blocks execution (T9).
- execution_id comes from the core hot record (T11).
"""
from typing import Any, Dict, List, Optional

import pytest

from orditect.flow import BaseBackEndTask, TaskExecutor
from orditect.flow.exceptions import TaskNotFoundError
from orditect.flow.snapshot import NullSnapshotSink


class FakeStorage:
    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(self, task_id: str, initial_status: str, **kw: Any) -> bool:
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "execution_id": f"exec-{task_id}",  # simulate core C3.5
            "progress": 0.0, "cancel_requested": False,
        }
        if kw.get("parent_task_id"):
            self._tasks[task_id]["parent_task_id"] = kw["parent_task_id"]
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


class SpySink:
    def __init__(self):
        self.writes: List[Dict[str, Any]] = []

    async def write(self, **kwargs: Any) -> None:
        self.writes.append(kwargs)


class FailingSink:
    async def write(self, **kwargs: Any) -> None:
        raise RuntimeError("sink down")


class QuickTask(BaseBackEndTask):
    def __init__(self, storage, governor=None, fail: Exception | None = None):
        super().__init__(storage, governor)
        self._fail = fail

    async def execute(self, task_id: str, **kwargs):
        if self._fail:
            raise self._fail
        return {"ok": True}


@pytest.mark.unit
class TestSnapshotSinkInjection:
    async def test_success_writes_running_and_succeeded(self):
        storage = FakeStorage()
        sink = SpySink()
        executor = TaskExecutor(storage, governor=None, snapshot_sink=sink)

        await storage.initialize_task("t1", "pending")
        await executor.execute(task_id="t1", task=QuickTask(storage))

        statuses = [(w["status"], w["terminal"]) for w in sink.writes]
        assert ("running", False) in statuses
        assert ("succeeded", True) in statuses
        # execution_id comes from the hot record (T11)
        assert all(w["execution_id"] == "exec-t1" for w in sink.writes)

    async def test_failure_writes_failed_terminal(self):
        storage = FakeStorage()
        sink = SpySink()
        executor = TaskExecutor(storage, governor=None, snapshot_sink=sink)

        await storage.initialize_task("t1", "pending")
        with pytest.raises(ValueError):
            await executor.execute(task_id="t1", task=QuickTask(storage, fail=ValueError("boom")))

        terminal_writes = [w for w in sink.writes if w["terminal"]]
        assert any(w["status"] == "failed" for w in terminal_writes)
        assert any("boom" in (w["error"] or "") for w in terminal_writes)

    async def test_sink_failure_does_not_block_execution(self):
        """T9: sink down -> task still succeeds."""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None, snapshot_sink=FailingSink())

        await storage.initialize_task("t1", "pending")
        result = await executor.execute(task_id="t1", task=QuickTask(storage))
        assert result == {"ok": True}

        stored = await storage.get_task("t1")
        assert stored["status"] == "succeeded"

    async def test_default_null_sink_zero_writes(self):
        """Default (no sink injected): NullSink, nothing recorded, no error."""
        storage = FakeStorage()
        executor = TaskExecutor(storage, governor=None)  # no snapshot_sink

        await storage.initialize_task("t1", "pending")
        result = await executor.execute(task_id="t1", task=QuickTask(storage))
        assert result == {"ok": True}
        assert isinstance(executor._snapshot_sink, NullSnapshotSink)