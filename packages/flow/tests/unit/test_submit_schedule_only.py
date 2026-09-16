"""Pinning tests for submit() schedule-only semantics (unit, in-memory).

Pins:
- An existing record still in its initial status is scheduled WITHOUT
  re-initialization (schedule-only).
- A get_task that raises for a missing record (non-conformant double)
  must not break the normal first-submission path.
- Pinned legacy overwrite: a non-pending existing record IS
  re-initialized (the fix diverts PENDING records only).
"""
import asyncio
import time
from typing import Any, Dict, List

import pytest

from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.flow.exceptions import TaskNotFoundError

pytestmark = pytest.mark.unit


class RaisingFakeStorage:
    """Non-conformant double: get_task raises for a missing task (the
    contract form is an empty dict). submit() must tolerate it."""

    def __init__(self):
        self._tasks: Dict[str, Dict[str, Any]] = {}

    async def initialize_task(self, task_id, initial_status, **kwargs) -> bool:
        if kwargs.get("if_not_exists") and task_id in self._tasks:
            return False
        self._tasks[task_id] = {
            "task_id": task_id,
            "status": initial_status,
            "cancel_requested": False,
            "progress": 0.0,
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

    async def list_children(self, parent_task_id):
        return []

    async def list_task_ids_by_status(self, status, **kwargs):
        return [tid for tid, t in self._tasks.items() if t.get("status") == status]

    async def bulk_get_tasks(self, task_ids):
        return [dict(self._tasks.get(tid, {})) for tid in task_ids]


class SpyInitStorage(RaisingFakeStorage):
    """Counts initialize_task calls to observe re-initialization."""

    def __init__(self):
        super().__init__()
        self.init_calls = 0

    async def initialize_task(self, task_id, initial_status, **kwargs) -> bool:
        self.init_calls += 1
        return await super().initialize_task(task_id, initial_status, **kwargs)


class _SpyTask(BaseBackEndTask):
    def __init__(self, storage, ran: List[str]):
        super().__init__(storage)
        self._ran = ran

    async def execute(self, task_id: str, **kwargs):
        self._ran.append(task_id)
        return {"ok": True}


async def _wait_terminal(storage, task_id: str, timeout: float = 3.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = await storage.get_task(task_id)
        if task["status"] in ("succeeded", "failed", "cancelled"):
            return task
        await asyncio.sleep(0.02)
    raise AssertionError(f"task {task_id} did not reach terminal state")


class TestSubmitScheduleOnly:
    async def test_existing_pending_schedule_only_no_reinit(self):
        """Existing pending record: scheduled without re-initialization."""
        storage = SpyInitStorage()
        await storage.initialize_task("t_pending", "pending")
        assert storage.init_calls == 1

        orchestrator = TaskOrchestrator(storage, governor=None)
        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_pending")

        record = await _wait_terminal(storage, "t_pending")
        assert record["status"] == "succeeded"
        assert ran == ["t_pending"]
        assert storage.init_calls == 1  # no re-initialization
        await orchestrator.wait_all_finalized()

    async def test_first_submission_with_raising_fake_storage(self):
        """A get_task that raises for a missing record must not break the
        normal first-submission path (contract tolerance)."""
        storage = RaisingFakeStorage()
        orchestrator = TaskOrchestrator(storage, governor=None)
        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_first")

        record = await _wait_terminal(storage, "t_first")
        assert record["status"] == "succeeded"
        assert ran == ["t_first"]
        await orchestrator.wait_all_finalized()

    async def test_existing_non_pending_reinitialized(self):
        """Pinned legacy overwrite: a non-pending existing record IS
        re-initialized (the fix diverts PENDING records only)."""
        storage = SpyInitStorage()
        await storage.initialize_task("t_legacy", "succeeded")
        assert storage.init_calls == 1

        orchestrator = TaskOrchestrator(storage, governor=None)
        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_legacy")

        record = await _wait_terminal(storage, "t_legacy")
        assert record["status"] == "succeeded"
        assert ran == ["t_legacy"]
        assert storage.init_calls == 2  # legacy overwrite preserved
        await orchestrator.wait_all_finalized()