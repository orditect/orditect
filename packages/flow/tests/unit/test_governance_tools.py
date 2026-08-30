
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

from fake_infra import FakeDepGraphStore, FakeGovernanceStorage
from orditect.protocol import DependencyEdge

pytestmark = pytest.mark.unit

def _gov(storage, **kwargs) -> DependencyGovernor:
    kwargs.setdefault("success_words", frozenset({"succeeded"}))
    return DependencyGovernor(storage, **kwargs)


# ---------- scan_dependency_cycles ----------

async def test_scan_acyclic_graph_clean():
    store = FakeDepGraphStore()
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="b", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="b", parent_id="c", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="d", is_primary=False))
    assert await scan_dependency_cycles(store) == []

async def test_scan_detects_simple_cycle():
    store = FakeDepGraphStore()
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="b", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="b", parent_id="a", is_primary=True))
    cycles = await scan_dependency_cycles(store)
    assert len(cycles) == 1
    assert set(cycles[0]) == {"a", "b"}

async def test_scan_detects_self_loop():
    store = FakeDepGraphStore()
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="a", is_primary=True))
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
    # ✅ 通过 write_dependency 写入 DependencyEdge 对象
    await store.write_dependency(DependencyEdge(child_id="c", parent_id="p1", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="c", parent_id="p2", is_primary=False))
    await store.write_dependency(DependencyEdge(child_id="c", parent_id="p3", is_primary=False))

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
    await store.write_dependency(
        DependencyEdge(child_id="ghost-child", parent_id="ghost-parent", is_primary=True)
    )

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

async def test_scan_deep_chain_no_recursion_error():
    """v0.1.6 pinning: a dependency chain far deeper than Python's recursion
    limit must not crash the offline scan.

    Red before: the recursive walk() blew past RecursionError around 1000
    frames — the offline scan (line 2 of cycle detection) collapsed exactly
    on the graphs it exists to protect.
    """
    store = FakeDepGraphStore()
    depth = 3000  # well past the default recursion limit (~1000)
    for i in range(depth):
        await store.write_dependency(
            DependencyEdge(child_id=f"n{i}", parent_id=f"n{i + 1}", is_primary=True)
        )

    cycles = await scan_dependency_cycles(store)
    assert cycles == []  # acyclic long chain must complete cleanly


async def test_scan_detects_cycle_beyond_register_dfs_depth():
    """Cycles deeper than the register-time walk bound (32) are exactly the
    blind spot this offline tool exists to close."""
    store = FakeDepGraphStore()
    depth = 100  # deeper than _MAX_LINEAGE_DEPTH (32)
    for i in range(depth):
        await store.write_dependency(
            DependencyEdge(child_id=f"n{i}", parent_id=f"n{i + 1}", is_primary=True)
        )
    await store.write_dependency(
        DependencyEdge(child_id=f"n{depth}", parent_id="n0", is_primary=True)
    )  # closes the cycle

    cycles = await scan_dependency_cycles(store)
    assert len(cycles) == 1
    assert set(cycles[0]) == {f"n{i}" for i in range(depth + 1)}


async def test_scan_shared_parent_no_false_cycle():
    """Diamonds (a node reached via two paths) must not be misreported as a
    cycle — the memoized-visited guard must survive the iterative rewrite."""
    store = FakeDepGraphStore()
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="b", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="a", parent_id="c", is_primary=False))
    await store.write_dependency(DependencyEdge(child_id="b", parent_id="d", is_primary=True))
    await store.write_dependency(DependencyEdge(child_id="c", parent_id="d", is_primary=True))

    assert await scan_dependency_cycles(store) == []