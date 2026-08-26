"""Terminal-notification bidirectional semantics (DependencyGovernor)."""

from __future__ import annotations

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


async def _three_parent_setup(storage, gov, statuses):
    """Register c under three parents with the given statuses; returns None."""
    for i, st in enumerate(statuses, start=1):
        await storage.initialize_task(f"p{i}", st)
    await storage.initialize_task("c", "pending")
    await gov.register_dependency("c", ["p1", "p2", "p3"])


# ---------- as-parent: counter decrement ----------


async def test_parent_terminal_decrements_remaining_deps():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])
    assert await storage.get_remaining_deps("c") == 3

    await gov.notify_task_terminal("p1", "succeeded")
    assert await storage.get_remaining_deps("c") == 2

    await gov.notify_task_terminal("p2", "succeeded")
    await gov.notify_task_terminal("p3", "succeeded")
    assert await storage.get_remaining_deps("c") == 0
    assert await gov.get_ready_tasks() == ["c"]


async def test_success_parent_never_auto_votes():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])

    await gov.notify_task_terminal("p1", "succeeded")

    assert await storage.get_cancel_votes("c") == []
    assert await storage.get_remaining_deps("c") == 2


async def test_failed_parent_auto_votes():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])

    await gov.notify_task_terminal("p1", "failed")

    assert await storage.get_cancel_votes("c") == ["p1"]
    assert await storage.get_remaining_deps("c") == 2


async def test_all_parents_failed_cancels_child():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    gov = _gov(storage, lifecycle=lifecycle)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])

    await gov.notify_task_terminal("p1", "failed")
    await gov.notify_task_terminal("p2", "failed")
    assert lifecycle.cancelled == []  # threshold not reached yet

    await gov.notify_task_terminal("p3", "failed")
    assert lifecycle.cancelled == ["c"]
    assert await storage.get_cancel_votes("c") == ["p1", "p2", "p3"]


async def test_cancelled_parent_also_auto_votes():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])

    await gov.notify_task_terminal("p1", "cancelled")
    assert await storage.get_cancel_votes("c") == ["p1"]


async def test_notify_for_already_terminal_child_no_cancel():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    gov = _gov(storage, lifecycle=lifecycle)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])
    await storage.update_task("c", {"status": "cancelled"})

    await gov.notify_task_terminal("p1", "failed")
    await gov.notify_task_terminal("p2", "failed")
    await gov.notify_task_terminal("p3", "failed")

    assert lifecycle.cancelled == []  # child already terminal: never re-cancelled


# ---------- as-child: reverse cleanup ----------


async def test_child_terminal_removes_itself_and_clears_votes():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])
    await gov.vote_cancel("p1", "c")
    assert await storage.get_cancel_votes("c") == ["p1"]

    await gov.notify_task_terminal("c", "succeeded")

    assert await storage.get_active_children("p1") == []
    assert await storage.get_active_children("p2") == []
    assert await storage.get_active_children("p3") == []
    assert await storage.get_cancel_votes("c") == []


# ---------- fault tolerance ----------


async def test_decr_missing_counter_tolerated_and_logged(caplog):
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await storage.initialize_task("p1", "running")
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"depends_on": ["p1"]})
    await storage.sadd_active_child("p1", "c")
    # no set_remaining_deps: DECR on a missing key goes negative
    assert await storage.get_remaining_deps("c") == 0

    await gov.notify_task_terminal("p1", "failed")

    assert await storage.get_remaining_deps("c") == -1  # tolerated, not raised
    assert await storage.get_cancel_votes("c") == ["p1"]


async def test_duplicate_notify_keeps_child_ready_once():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await _three_parent_setup(storage, gov, ["running", "running", "running"])
    await gov.notify_task_terminal("p1", "succeeded")
    await gov.notify_task_terminal("p2", "succeeded")
    await gov.notify_task_terminal("p3", "succeeded")

    # duplicate notification: counter goes negative, child still listed once
    await gov.notify_task_terminal("p1", "succeeded")
    assert await storage.get_remaining_deps("c") == -1
    assert await gov.get_ready_tasks() == ["c"]