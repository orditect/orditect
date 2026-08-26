"""Offline tools: scan_dependency_cycles / rebuild_dep_counters / recovery wiring."""

from __future__ import annotations

import asyncio

import pytest

from orditect.flow.governance import (
    DependencyGovernor,
    rebuild_dep_counters,
    scan_dependency_cycles,
)
from orditect.flow.recovery.service import RecoveryService

from fake_infra import FakeGovernanceStorage

pytestmark = pytest.mark.unit


class FakeDepGraphStore:
    def __init__(self) -> None:
        self.edges: list[tuple[str, str, bool]] = []

    async def write_dependency(self, child_id: str, parent_id: str, is_primary: bool) -> None:
        self.edges.append((child_id, parent_id, is_primary))

    async def read_graph(self, root_id: str) -> dict:
        return {"nodes": [], "edges": []}

    async def all_edges(self) -> list[tuple[str, str]]:
        return [(c, p) for c, p, _ in self.edges]


def _gov(storage, **kwargs) -> DependencyGovernor:
    kwargs.setdefault("success_words", frozenset({"succeeded"}))
    return DependencyGovernor(storage, **kwargs)


# ---------- scan_dependency_cycles ----------


async def test_scan_acyclic_graph_clean():
    store = FakeDepGraphStore()
    store.edges.extend([("a", "b", True), ("b", "c", True), ("a", "d", False)])
    assert await scan_dependency_cycles(store) == []


async def test_scan_detects_simple_cycle():
    store = FakeDepGraphStore()
    store.edges.extend([("a", "b", True), ("b", "a", True)])
    cycles = await scan_dependency_cycles(store)
    assert len(cycles) == 1
    assert set(cycles[0]) == {"a", "b"}


async def test_scan_detects_self_loop():
    store = FakeDepGraphStore()
    store.edges.append(("a", "a", True))
    cycles = await scan_dependency_cycles(store)
    assert len(cycles) == 1
    assert cycles[0][0] == "a"


# ---------- rebuild_dep_counters ----------


async def test_rebuild_recomputes_counters_and_votes():
    storage = FakeGovernanceStorage()
    store = FakeDepGraphStore()
    for tid, st in [("p1", "running"), ("p2", "failed"),
                    ("p3", "succeeded"), ("c", "pending")]:
        await storage.initialize_task(tid, st)
    store.edges.extend([("c", "p1", True), ("c", "p2", False), ("c", "p3", False)])

    stats = await rebuild_dep_counters(storage, store)

    assert stats["rebuilt"] == 1
    assert stats["errors"] == 0
    assert await storage.get_remaining_deps("c") == 1
    assert await storage.get_cancel_votes("c") == ["p2"]
    assert await storage.get_active_children("p1") == ["c"]
    assert await storage.get_active_children("p2") == []


async def test_rebuild_skips_missing_hot_records():
    storage = FakeGovernanceStorage()
    store = FakeDepGraphStore()
    store.edges.append(("ghost-child", "ghost-parent", True))

    stats = await rebuild_dep_counters(storage, store)
    assert stats["skipped"] == 1
    assert stats["rebuilt"] == 0


# ---------- recovery wiring ----------


class _FakeReader:
    def __init__(self, tree_ids):
        self._tree_ids = tree_ids

    async def get_tree(self, root_task_id: str, latest_only: bool = True):
        class S:
            def __init__(self, tid):
                self.task_id = tid
        return [S(t) for t in self._tree_ids]

    async def get(self, task_id: str, step: str):
        return None  # no success snapshot: every node reruns


class _ImmediateExecutor:
    """Runs task.execute inline (no background task) for deterministic tests."""

    def __init__(self, storage):
        self._storage = storage
        self.executed: list[str] = []

    async def execute(self, task_id: str, task, **kwargs):
        self.executed.append(task_id)
        await task.execute(task_id=task_id, **kwargs)


class _NoopTask:
    async def execute(self, task_id: str, **kwargs):
        return None

async def _noop_task_factory(task_id: str):
    return _NoopTask()


async def _wait_for(predicate, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def test_recovery_rerun_invalidates_snapshot():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await storage.initialize_task("c", "failed")
    await storage.update_task(
        "c",
        {
            "depends_on": ["p1"],
            "primary_parent": "p1",
            "exempt_resources_snapshot": ["llm"],
        },
    )

    executor = _ImmediateExecutor(storage)
    svc = RecoveryService(
        storage=storage,
        snapshot_reader=_FakeReader(["c"]),
        executor=executor,
        reuse_terminal_words=frozenset({"succeeded"}),
        task_factory=_noop_task_factory,
        dependency_governor=gov,
    )

    plan = await svc.rerun("root", scope={"c"})
    assert plan["c"].value == "rerun"

    await _wait_for(lambda: executor.executed == ["c"])
    rec = await storage.get_task("c")
    assert rec["exempt_resources_snapshot"] is None  # invalidated after reopen
    assert rec["depends_on"] == ["p1"]               # dependency metadata kept
    assert rec["status"] == "pending"                # reopened initial state
    assert len(rec["previous_execution_ids"]) == 1   # generation advanced


async def test_recovery_without_governor_keeps_snapshot():
    storage = FakeGovernanceStorage()
    await storage.initialize_task("c", "failed")
    await storage.update_task("c", {"exempt_resources_snapshot": ["llm"]})

    executor = _ImmediateExecutor(storage)
    svc = RecoveryService(
        storage=storage,
        snapshot_reader=_FakeReader(["c"]),
        executor=executor,
        reuse_terminal_words=frozenset({"succeeded"}),
        task_factory=_noop_task_factory,
    )

    await svc.rerun("root", scope={"c"})
    await _wait_for(lambda: executor.executed == ["c"])
    rec = await storage.get_task("c")
    assert rec["exempt_resources_snapshot"] == ["llm"]  # untouched w/o governor