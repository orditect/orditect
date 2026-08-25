"""Behavioral tests for snapshot tree traversal and aggregation semantics
(beyond the conformance suite's coverage)."""

import asyncio

import pytest

from orditect.adapter.memory import MemoryStore
from orditect.protocol import TaskSnapshot


def _snap(tid, step, eid, parent=None, status="running", cost=None):
    return TaskSnapshot(
        task_id=tid, step=step, execution_id=eid,
        parent_task_id=parent, status=status, cost=cost,
    )


@pytest.mark.unit
class TestTreeTraversal:
    async def test_get_tree_full_subtree(self):
        store = MemoryStore().snapshot
        await store.save(_snap("root", "s", "e1"))
        await store.save(_snap("a", "s", "e1", parent="root"))
        await store.save(_snap("b", "s", "e1", parent="root"))
        await store.save(_snap("a1", "s", "e1", parent="a"))

        tree = await store.get_tree("root")
        ids = {s.task_id for s in tree}
        assert ids == {"root", "a", "b", "a1"}

    async def test_get_tree_depth_bounded(self):
        store = MemoryStore().snapshot
        await store.save(_snap("root", "s", "e1"))
        await store.save(_snap("a", "s", "e1", parent="root"))
        await store.save(_snap("a1", "s", "e1", parent="a"))

        tree = await store.get_tree("root", max_depth=1)
        ids = {s.task_id for s in tree}
        assert "a1" not in ids

    async def test_get_tree_cycle_safe(self):
        store = MemoryStore().snapshot
        await store.save(_snap("a", "s", "e1"))
        await store.save(_snap("b", "s", "e1", parent="a"))
        await store.save(_snap("a", "s", "e2", parent="b"))  # cycle a<-b<-a

        # Must terminate, not loop
        tree = await asyncio.wait_for(store.get_tree("a", latest_only=False), timeout=2.0)
        assert len(tree) > 0

    async def test_get_ancestors_root_first(self):
        store = MemoryStore().snapshot
        await store.save(_snap("root", "s", "e1"))
        await store.save(_snap("mid", "s", "e1", parent="root"))
        await store.save(_snap("leaf", "s", "e1", parent="mid"))

        chain = await store.get_ancestors("leaf")
        assert [s.task_id for s in chain] == ["root", "mid"]

    async def test_get_ancestors_cycle_safe(self):
        store = MemoryStore().snapshot
        await store.save(_snap("a", "s", "e1", parent="b"))
        await store.save(_snap("b", "s", "e1", parent="a"))  # cycle

        chain = await asyncio.wait_for(store.get_ancestors("a"), timeout=2.0)
        assert isinstance(chain, list)


@pytest.mark.unit
class TestAggregate:
    async def test_aggregate_by_status(self):
        store = MemoryStore().snapshot
        await store.save(_snap("t1", "s", "e1", status="done", cost={"usd": 0.1}))
        await store.save(_snap("t2", "s", "e1", status="done", cost={"usd": 0.2}))
        await store.save(_snap("t3", "s", "e1", status="running"))

        out = await store.aggregate(group_by="status")
        assert out["done"]["count"] == 2
        assert out["done"]["cost"]["usd"] == pytest.approx(0.3)
        assert out["running"]["count"] == 1