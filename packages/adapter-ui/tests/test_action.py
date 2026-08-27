"""Pinning tests for the command-queue action sink (DD-013 form).

Verifies: commands are enqueued (not directly executed), audit events are
written with event_id == action_id, receipts are queryable, and the
dispatcher executes commands asynchronously via flow's public API.
"""

import asyncio

import pytest

from orditect.adapter.ui import (
    ActionSinkAdapter,
    MemoryActionQueue,
)
from orditect.flow import (
    BaseBackEndTask,
    RecoveryService,
    TaskOrchestrator,
)
from orditect.flow.actions import ActionDispatcher, ActionType

pytestmark = pytest.mark.unit

class FakeStorage:
    def __init__(self):
        self._tasks = {}

    async def initialize_task(self, task_id, initial_status, **kw):
        self._tasks[task_id] = {
            "task_id": task_id, "status": initial_status,
            "cancel_requested": False,
            "execution_id": f"exec-{task_id}",
        }
        return True

    async def update_task(self, task_id, updates, **kwargs):
        if task_id in self._tasks:
            self._tasks[task_id].update(updates)

    async def get_task(self, task_id):
        return dict(self._tasks.get(task_id, {}))

    async def request_cancel(self, task_id):
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True

    async def list_children(self, parent_task_id):
        return [
            tid for tid, t in self._tasks.items()
            if t.get("parent_task_id") == parent_task_id
        ]

    async def reopen_task(self, task_id, **kw):
        new_eid = f"exec-re-{task_id}"
        self._tasks[task_id]["execution_id"] = new_eid
        self._tasks[task_id]["status"] = "pending"
        self._tasks[task_id].pop("result", None)
        return new_eid


class FakeReader:
    def __init__(self, statuses, tree_ids):
        self._statuses = statuses
        self._tree_ids = tree_ids

    async def get(self, task_id, step="execute"):
        st = self._statuses.get(task_id)
        if st is None:
            return None

        class S:
            def __init__(self, tid, status):
                self.task_id = tid
                self.status = status

        return S(task_id, st)

    async def get_tree(self, root_task_id, **kw):
        return [
            type("S", (), {"task_id": t, "status": self._statuses.get(t, "")})()
            for t in self._tree_ids
        ]


class FakeExecutor:
    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, task_id, task, **kw):
        self.executed.append(task_id)
        return {"re": task_id}


class _NoopTask:
    async def execute(self, task_id, **kwargs):
        return None


async def _factory(task_id):
    return _NoopTask()


class RecordingAudit:
    def __init__(self):
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)


async def _wait_for(predicate, timeout=3.0, interval=0.02):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


class TestActionSinkQueueing:
    async def test_pause_enqueues_command_not_direct_execution(self):
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)

        receipt = await sink.pause_node("t1", actor="user-1")
        assert receipt.accepted is True
        assert receipt.action_id.startswith("act-")

        # command is in the queue, not yet executed
        command = await queue.dequeue(timeout=0.1)
        assert command is not None
        assert command.action_type is ActionType.PAUSE
        assert command.target_task_id == "t1"
        assert command.actor == "user-1"

    async def test_audit_event_written_with_action_id(self):
        queue = MemoryActionQueue()
        audit = RecordingAudit()
        sink = ActionSinkAdapter(queue, audit_writer=audit)

        receipt = await sink.pause_node("t1")
        assert receipt.accepted is True

        assert len(audit.events) == 1
        ev = audit.events[0]
        assert ev.event_id == receipt.action_id
        assert ev.event_type == "action_pause"
        assert ev.task_id == "t1"

    async def test_retry_scope_enqueues_with_scope(self):
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)

        receipt = await sink.retry_scope("root", {"a", "b"})
        assert receipt.accepted is True

        command = await queue.dequeue(timeout=0.1)
        assert command.action_type is ActionType.RETRY
        assert command.root_task_id == "root"
        assert command.scope == frozenset({"a", "b"})


class TestActionDispatcher:
    async def test_pause_executed_via_dispatcher(self):
        storage = FakeStorage()
        await storage.initialize_task("t1", "running")
        orch = TaskOrchestrator(storage, governor=None)

        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)
        dispatcher = ActionDispatcher(queue, orch, recovery=None)

        await dispatcher.start()
        try:
            receipt = await sink.pause_node("t1")
            assert receipt.accepted is True

            # wait for dispatcher to execute
            assert await _wait_for(
                lambda: queue._receipts.get(receipt.action_id) is not None
            )

            exec_receipt = queue._receipts[receipt.action_id]
            assert exec_receipt["status"] == "executed"
            assert "cancelled" in exec_receipt["detail"]

            # verify the task was actually cancelled
            rec = await storage.get_task("t1")
            assert rec["cancel_requested"] is True
        finally:
            await dispatcher.stop()

    async def test_resume_executed_via_dispatcher(self):
        storage = FakeStorage()
        storage._tasks["root"] = {
            "task_id": "root", "status": "succeeded",
            "result": {"r": 1}, "execution_id": "e1",
        }
        storage._tasks["bad"] = {
            "task_id": "bad", "status": "failed", "execution_id": "e1",
        }
        reader = FakeReader(
            statuses={"root": "succeeded", "bad": "failed"},
            tree_ids=["root", "bad"],
        )
        executor = FakeExecutor()
        recovery = RecoveryService(
            storage, reader, executor,
            reuse_terminal_words=frozenset({"succeeded"}),
            task_factory=_factory,
        )

        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)
        dispatcher = ActionDispatcher(
            queue, orchestrator=None, recovery=recovery
        )

        await dispatcher.start()
        try:
            receipt = await sink.resume_tree("root")
            assert receipt.accepted is True

            assert await _wait_for(
                lambda: queue._receipts.get(receipt.action_id) is not None
            )

            exec_receipt = queue._receipts[receipt.action_id]
            assert exec_receipt["status"] == "executed"
            assert "reuse=1" in exec_receipt["detail"]
            assert "rerun=1" in exec_receipt["detail"]

            # wait for recovery's background task
            assert await _wait_for(lambda: bool(executor.executed))
            assert "bad" in executor.executed
        finally:
            await dispatcher.stop()

    async def test_idempotent_action_dedup(self):
        storage = FakeStorage()
        await storage.initialize_task("t1", "running")
        orch = TaskOrchestrator(storage, governor=None)

        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)
        dispatcher = ActionDispatcher(queue, orch, recovery=None)

        await dispatcher.start()
        try:
            receipt1 = await sink.pause_node("t1")
            receipt2 = await sink.pause_node("t1")
            # two different action_ids (each submit generates a new one)
            assert receipt1.action_id != receipt2.action_id

            # but if we manually re-enqueue the same command, dedup kicks in
            command = await queue.dequeue(timeout=0.1)
            await queue.enqueue(command)  # re-enqueue same command
            await queue.enqueue(command)  # and again

            assert await _wait_for(
                lambda: queue._receipts.get(command.action_id) is not None
            )
            # only one receipt despite 3 enqueues
            assert len(queue._receipts) == 1
        finally:
            await dispatcher.stop()