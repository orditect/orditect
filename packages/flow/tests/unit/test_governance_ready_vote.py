"""Readiness query and cancel-vote semantics (DependencyGovernor)."""

from __future__ import annotations

import asyncio

import pytest

from orditect.flow.governance import DependencyGovernor

from fake_infra import FakeGovernanceStorage

pytestmark = pytest.mark.unit


class RecordingLifecycle:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def cancel(self, task_id: str) -> bool:
        self.cancelled.append(task_id)
        return True


def _gov(storage, **kwargs) -> DependencyGovernor:
    kwargs.setdefault("success_words", frozenset({"succeeded"}))
    return DependencyGovernor(storage, **kwargs)


# ---------- get_ready_tasks ----------


async def test_ready_tasks_pending_only():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("a", "pending")
    await storage.initialize_task("b", "pending")
    await storage.set_remaining_deps("a", 0)
    await storage.set_remaining_deps("b", 1)

    gov = _gov(storage)
    assert await gov.get_ready_tasks() == ["a"]

    await storage.decr_remaining_deps("b")
    assert await gov.get_ready_tasks() == ["a", "b"]


async def test_ready_status_custom_vocabulary():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("a", "created")  # external vocabulary
    await storage.set_remaining_deps("a", 0)

    gov = _gov(storage, ready_status="created")
    assert await gov.get_ready_tasks() == ["a"]

    gov_default = _gov(storage)
    assert await gov_default.get_ready_tasks() == []


# ---------- vote_cancel ----------


async def test_vote_cancel_unregistered_parent_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    await storage.initialize_task("p1", "running")
    await storage.update_task("c", {"depends_on": ["p1"]})

    gov = _gov(storage)
    assert await gov.vote_cancel("stranger", "c") is False
    assert await storage.get_cancel_votes("c") == []


async def test_vote_cancel_terminal_child_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "succeeded")
    await storage.update_task("c", {"depends_on": ["p1"]})

    gov = _gov(storage)
    assert await gov.vote_cancel("p1", "c") is False


async def test_vote_cancel_missing_child_rejected():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    assert await gov.vote_cancel("p1", "ghost") is False


async def test_vote_cancel_threshold_triggers_cancel():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"depends_on": ["p1", "p2", "p3"]})

    gov = _gov(storage, lifecycle=lifecycle)
    assert await gov.vote_cancel("p1", "c") is False
    assert await gov.vote_cancel("p2", "c") is False
    assert await gov.vote_cancel("p3", "c") is True
    assert lifecycle.cancelled == ["c"]


async def test_vote_cancel_without_lifecycle_records_only():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"depends_on": ["p1", "p2"]})

    gov = _gov(storage)  # no lifecycle: votes recorded, nothing triggered
    assert await gov.vote_cancel("p1", "c") is False
    assert await gov.vote_cancel("p2", "c") is True
    assert await storage.get_cancel_votes("c") == ["p1", "p2"]


async def test_vote_cancel_repeat_same_parent_idempotent():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"depends_on": ["p1", "p2"]})

    gov = _gov(storage, lifecycle=lifecycle)
    assert await gov.vote_cancel("p1", "c") is False
    assert await gov.vote_cancel("p1", "c") is False  # same voter twice
    assert await storage.get_cancel_votes("c") == ["p1"]


async def test_concurrent_votes_exactly_one_cancel():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"depends_on": ["p1", "p2", "p3"]})

    gov = _gov(storage, lifecycle=lifecycle)
    results = await asyncio.gather(
        *(gov.vote_cancel(p, "c") for p in ("p1", "p2", "p3"))
    )
    assert results.count(True) == 1
    assert lifecycle.cancelled == ["c"]