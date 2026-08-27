"""Registration semantics matrix for DependencyGovernor.register_dependency."""

from __future__ import annotations

import pytest

from orditect.flow.exceptions import TaskNotFoundError
from orditect.flow.governance import DependencyGovernor

from fake_infra import FakeDepGraphStore, FakeGovernanceStorage

pytestmark = pytest.mark.unit


# ---------- test-local doubles ----------


class RecordingLifecycle:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    async def cancel(self, task_id: str) -> bool:
        self.cancelled.append(task_id)
        return True


class RecordingAuditWriter:
    def __init__(self) -> None:
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)

#
# class FakeDepGraphStore:
#     def __init__(self, fail: bool = False) -> None:
#         self.edges: list[tuple[str, str, bool]] = []
#         self._fail = fail
#
#     async def write_dependency(self, child_id: str, parent_id: str, is_primary: bool) -> None:
#         if self._fail:
#             raise RuntimeError("cold store down")
#         self.edges.append((child_id, parent_id, is_primary))
#
#     async def read_graph(self, root_id: str) -> dict:
#         return {"nodes": [], "edges": []}
#
#     async def all_edges(self) -> list[tuple[str, str]]:
#         return [(c, p) for c, p, _ in self.edges]


class MidRegisterFlipStorage(FakeGovernanceStorage):
    """Flips a parent's status the moment the child's hot record is written
    (i.e., after classification, before the post-write recheck) — simulates
    a parent terminating mid-registration with its notify missing the child."""

    def __init__(self, child_id: str, flip_target: str, flip_to: str) -> None:
        super().__init__()
        self._child = child_id
        self._flip_target = flip_target
        self._flip_to = flip_to
        self._flipped = False

    async def update_task(self, task_id, updates, **kwargs):
        await super().update_task(task_id, updates, **kwargs)
        if task_id == self._child and not self._flipped:
            self._flipped = True
            await super().update_task(self._flip_target, {"status": self._flip_to})


def _gov(storage, **kwargs) -> DependencyGovernor:
    kwargs.setdefault("success_words", frozenset({"succeeded"}))
    return DependencyGovernor(storage, **kwargs)


# ---------- classification matrix ----------


async def test_two_running_one_succeeded_parent():
    storage = FakeGovernanceStorage()
    for tid, st in [("p1", "running"), ("p2", "running"),
                    ("p3", "succeeded"), ("c", "pending")]:
        await storage.initialize_task(tid, st)

    await _gov(storage).register_dependency("c", ["p1", "p2", "p3"])

    assert await storage.get_remaining_deps("c") == 2
    assert await storage.get_cancel_votes("c") == []
    assert await storage.get_active_children("p1") == ["c"]
    assert await storage.get_active_children("p2") == ["c"]
    assert await storage.get_active_children("p3") == []
    rec = await storage.get_task("c")
    assert rec["depends_on"] == ["p1", "p2", "p3"]
    assert rec["primary_parent"] == "p1"
    assert rec["exempt_resources_snapshot"] == []


async def test_running_failed_succeeded_parent_mix():
    storage = FakeGovernanceStorage()
    for tid, st in [("p1", "running"), ("p2", "failed"),
                    ("p3", "succeeded"), ("c", "pending")]:
        await storage.initialize_task(tid, st)

    await _gov(storage).register_dependency("c", ["p1", "p2", "p3"])

    assert await storage.get_remaining_deps("c") == 1
    assert await storage.get_cancel_votes("c") == ["p2"]
    assert await storage.get_active_children("p2") == []


async def test_all_parents_failed_triggers_cancel():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    for tid, st in [("p1", "failed"), ("p2", "failed"), ("c", "pending")]:
        await storage.initialize_task(tid, st)

    await _gov(storage, lifecycle=lifecycle).register_dependency("c", ["p1", "p2"])

    assert lifecycle.cancelled == ["c"]
    assert await storage.get_cancel_votes("c") == ["p1", "p2"]
    assert await storage.get_remaining_deps("c") == 0


async def test_all_parents_succeeded_ready_not_scheduled():
    storage = FakeGovernanceStorage()
    lifecycle = RecordingLifecycle()
    for tid, st in [("p1", "succeeded"), ("p2", "succeeded"), ("c", "pending")]:
        await storage.initialize_task(tid, st)

    await _gov(storage, lifecycle=lifecycle).register_dependency("c", ["p1", "p2"])

    assert await storage.get_remaining_deps("c") == 0
    assert lifecycle.cancelled == []  # success never auto-votes
    assert await storage.list_ready_dep_tasks(status="pending") == ["c"]


# ---------- validation ----------


async def test_missing_child_raises():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("p1", "running")
    with pytest.raises(TaskNotFoundError):
        await _gov(storage).register_dependency("ghost", ["p1"])


async def test_missing_parent_raises():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    with pytest.raises(TaskNotFoundError):
        await _gov(storage).register_dependency("c", ["ghost"])


async def test_empty_parents_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency("c", [])


async def test_primary_parent_must_be_in_parents():
    storage = FakeGovernanceStorage()
    for tid in ("c", "p1"):
        await storage.initialize_task(tid, "pending")
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency("c", ["p1"], primary_parent="nope")


# ---------- cycle detection ----------


async def test_cycle_via_depends_on_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    await storage.initialize_task("p1", "running")
    await storage.update_task("p1", {"depends_on": ["c"]})
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency("c", ["p1"])


async def test_cycle_via_parent_task_id_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    await storage.initialize_task("p1", "running", parent_task_id="c")
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency("c", ["p1"])


async def test_self_dependency_rejected():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "pending")
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency("c", ["c"])


# ---------- exemption snapshot ----------


async def test_snapshot_explicit_cap():
    storage = FakeGovernanceStorage()
    for tid in ("c", "p1"):
        await storage.initialize_task(tid, "pending")
    with pytest.raises(ValueError):
        await _gov(storage).register_dependency(
            "c", ["p1"], exempt_resources=[f"r{i}" for i in range(11)]
        )


async def test_snapshot_explicit_used_directly():
    storage = FakeGovernanceStorage()
    for tid in ("c", "p1"):
        await storage.initialize_task(tid, "pending")
    await _gov(storage).register_dependency("c", ["p1"], exempt_resources=["gpu"])
    rec = await storage.get_task("c")
    assert rec["exempt_resources_snapshot"] == ["gpu"]


async def test_snapshot_inherited_from_primary_chain():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("gp", "running")
    await storage.update_task("gp", {"resource": "task_execution"})
    await storage.initialize_task("p1", "running", parent_task_id="gp")
    await storage.update_task("p1", {"resource": "llm"})
    await storage.initialize_task("c", "pending")

    await _gov(storage).register_dependency("c", ["p1"])

    rec = await storage.get_task("c")
    assert rec["exempt_resources_snapshot"] == ["llm", "task_execution"]


# ---------- idempotency / TOCTOU compensation ----------


async def test_retry_same_registration_idempotent():
    storage = FakeGovernanceStorage()
    for tid, st in [("p1", "running"), ("p2", "running"),
                    ("p3", "succeeded"), ("c", "pending")]:
        await storage.initialize_task(tid, st)
    gov = _gov(storage)

    await gov.register_dependency("c", ["p1", "p2", "p3"])
    await gov.register_dependency("c", ["p1", "p2", "p3"])  # retry

    assert await storage.get_remaining_deps("c") == 2  # not 4
    assert await storage.get_cancel_votes("c") == []
    assert await storage.get_active_children("p1") == ["c"]


async def test_compensation_parent_failed_mid_registration():
    storage = MidRegisterFlipStorage("c", "p1", "failed")
    lifecycle = RecordingLifecycle()
    await storage.initialize_task("p1", "running")
    await storage.initialize_task("c", "pending")

    await _gov(storage, lifecycle=lifecycle).register_dependency("c", ["p1"])

    assert await storage.get_remaining_deps("c") == 0
    assert await storage.get_cancel_votes("c") == ["p1"]
    assert lifecycle.cancelled == ["c"]


async def test_compensation_parent_succeeded_mid_registration():
    storage = MidRegisterFlipStorage("c", "p1", "succeeded")
    lifecycle = RecordingLifecycle()
    await storage.initialize_task("p1", "running")
    await storage.initialize_task("c", "pending")

    await _gov(storage, lifecycle=lifecycle).register_dependency("c", ["p1"])

    assert await storage.get_remaining_deps("c") == 0
    assert await storage.get_cancel_votes("c") == []
    assert lifecycle.cancelled == []


# ---------- cold path ----------


async def test_cold_path_records_edges_with_primary_flag():
    storage = FakeGovernanceStorage()
    store = FakeDepGraphStore()
    for tid, st in [("p1", "running"), ("p2", "running"), ("c", "pending")]:
        await storage.initialize_task(tid, st)

    await _gov(storage, dep_graph_store=store).register_dependency(
        "c", ["p1", "p2"], primary_parent="p2"
    )

    assert sorted(store.edges) == [("c", "p1", False), ("c", "p2", True)]


async def test_cold_path_failure_degrades_to_audit():
    storage = FakeGovernanceStorage()
    store = FakeDepGraphStore(fail=True)
    audit = RecordingAuditWriter()
    await storage.initialize_task("p1", "running")
    await storage.initialize_task("c", "pending")

    # registration still succeeds despite the cold store being down
    await _gov(storage, dep_graph_store=store, audit_writer=audit).register_dependency(
        "c", ["p1"]
    )

    assert await storage.get_remaining_deps("c") == 1
    failed_events = [e for e in audit.events if e.event_type == "dep_index_write_failed"]
    assert len(failed_events) == 1
    assert failed_events[0].payload["parent"] == "p1"