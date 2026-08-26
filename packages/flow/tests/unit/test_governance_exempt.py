"""Exemption snapshot pre-branch tests (executor, v0.1.1).

Pins: frozen snapshot wins over the live ancestor walk; an empty snapshot
is explicit "no exemption"; a missing/None snapshot falls back to the walk;
the executor emits zero dependency-notification calls (contract 1).
"""

from __future__ import annotations

import pytest

from orditect.flow.core.executor import TaskExecutor
from orditect.flow.core.task import BaseBackEndTask

from fake_infra import FakeGovernanceStorage

pytestmark = pytest.mark.unit


class RecordingGovernor:
    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        self.acquired.append(resource)
        return f"tok-{resource}"

    async def try_acquire(self, resource: str):
        self.acquired.append(resource)
        return f"tok-{resource}"

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


class EchoTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs):
        return {"task_id": task_id}


class NoNotifyStorage(FakeGovernanceStorage):
    """Records any dependency-notification primitive touched by the executor."""

    def __init__(self) -> None:
        super().__init__()
        self.notify_calls: list[str] = []

    async def sadd_active_child(self, parent_id, child_id):
        self.notify_calls.append("sadd_active_child")
        await super().sadd_active_child(parent_id, child_id)

    async def srem_active_child(self, parent_id, child_id):
        self.notify_calls.append("srem_active_child")
        await super().srem_active_child(parent_id, child_id)

    async def set_remaining_deps(self, task_id, n):
        self.notify_calls.append("set_remaining_deps")
        await super().set_remaining_deps(task_id, n)

    async def decr_remaining_deps(self, task_id):
        self.notify_calls.append("decr_remaining_deps")
        return await super().decr_remaining_deps(task_id)

    async def vote_and_check_threshold(self, child_id, parent_id, threshold):
        self.notify_calls.append("vote_and_check_threshold")
        return await super().vote_and_check_threshold(child_id, parent_id, threshold)

    async def clear_cancel_votes(self, child_id):
        self.notify_calls.append("clear_cancel_votes")
        await super().clear_cancel_votes(child_id)


async def test_snapshot_match_exempts_acquire():
    storage = FakeGovernanceStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("c", "queued")
    await storage.update_task("c", {"exempt_resources_snapshot": ["llm"]})

    executor = TaskExecutor(storage, governor)
    result = await executor.execute("c", EchoTask(storage), resource="llm")

    assert result == {"task_id": "c"}
    assert governor.acquired == []  # exempted by the frozen snapshot
    assert governor.released == []  # inherited quota is never released here


async def test_snapshot_mismatch_acquires_and_releases():
    storage = FakeGovernanceStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("c", "queued")
    await storage.update_task("c", {"exempt_resources_snapshot": ["other"]})

    executor = TaskExecutor(storage, governor)
    await executor.execute("c", EchoTask(storage), resource="llm")

    assert governor.acquired == ["llm"]
    assert governor.released == ["llm"]
    rec = await storage.get_task("c")
    assert rec["resource"] == "llm"  # ledger written on real acquire


async def test_empty_snapshot_is_explicit_no_exemption():
    storage = FakeGovernanceStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("p", "running")
    await storage.update_task("p", {"resource": "llm"})
    await storage.initialize_task("c", "queued", parent_task_id="p")
    # explicit empty list: the walk would find "llm", the snapshot must win
    await storage.update_task("c", {"exempt_resources_snapshot": []})

    executor = TaskExecutor(storage, governor)
    await executor.execute("c", EchoTask(storage), resource="llm")

    assert governor.acquired == ["llm"]
    assert governor.released == ["llm"]


async def test_no_snapshot_falls_back_to_ancestor_walk():
    storage = FakeGovernanceStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("p", "running")
    await storage.update_task("p", {"resource": "llm"})
    await storage.initialize_task("c", "queued", parent_task_id="p")
    # no snapshot field: existing ancestor-walk exemption must be preserved

    executor = TaskExecutor(storage, governor)
    await executor.execute("c", EchoTask(storage), resource="llm")

    assert governor.acquired == []
    assert governor.released == []


async def test_none_snapshot_falls_back_to_ancestor_walk():
    storage = FakeGovernanceStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("p", "running")
    await storage.update_task("p", {"resource": "llm"})
    await storage.initialize_task("c", "queued", parent_task_id="p")
    # invalidated snapshot (invalidate_exempt_snapshot writes None)
    await storage.update_task("c", {"exempt_resources_snapshot": None})

    executor = TaskExecutor(storage, governor)
    await executor.execute("c", EchoTask(storage), resource="llm")

    assert governor.acquired == []  # walk exempts via the parent's ledger
    assert governor.released == []


async def test_executor_emits_zero_dependency_notifications():
    storage = NoNotifyStorage()
    governor = RecordingGovernor()
    await storage.initialize_task("t", "queued")

    executor = TaskExecutor(storage, governor)
    await executor.execute("t", EchoTask(storage), resource="llm")

    # orchestration independence: readiness is driven exclusively by
    # external notify_task_terminal() calls, never by the executor.
    assert storage.notify_calls == []