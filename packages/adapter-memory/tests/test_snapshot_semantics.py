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

@pytest.mark.unit
class TestNonStateMerge:
    """CF-SNP-013 mirror: sparse same-generation save must not erase
    previously recorded non-state fields (v0.1.4)."""

    async def test_sparse_resave_preserves_cost(self):
        store = MemoryStore().snapshot
        await store.save(_snap("t", "s", "e1", "running", cost={"usd": 0.5}))
        await store.save_terminal(_snap("t", "s", "e1", "done", cost={"usd": 0.5}))
        # sparse same-generation re-save without cost
        await store.save(_snap("t", "s", "e1", "done"))

        got = await store.get("t", "s")
        assert got is not None
        assert got.cost == {"usd": 0.5}

@pytest.mark.unit
class TestSparseSaveSemantics:
    """CF-SNP-014 mirror: empty-status sparse saves never regress status,
    never drift created_at, and still merge non-state fields (v0.1.5)."""

    async def test_sparse_save_preserves_status_and_created_at(self):
        store = MemoryStore().snapshot
        first = _snap("t", "s", "e1", status="running", cost={"usd": 0.1})
        await store.save(first)
        sparse = _snap("t", "s", "e1").model_copy(
            update={"status": "", "error": "boom"}
        )
        await store.save(sparse)

        got = await store.get("t", "s")
        assert got is not None
        assert got.status == "running"
        assert got.cost == {"usd": 0.1}
        assert got.error == "boom"
        assert got.created_at == first.created_at

    async def test_statusless_save_after_terminal_is_legal(self):
        store = MemoryStore().snapshot
        await store.save_terminal(
            _snap("t", "s", "e1", status="done", cost={"usd": 0.1})
        )
        sparse = _snap("t", "s", "e1").model_copy(
            update={"status": "", "model": "m1"}
        )
        await store.save(sparse)

        got = await store.get("t", "s")
        assert got is not None
        assert got.status == "done"
        assert got.model == "m1"

    async def test_explicit_status_change_after_terminal_still_rejected(self):
        from orditect.protocol import TerminalStateViolationError

        store = MemoryStore().snapshot
        await store.save_terminal(_snap("t", "s", "e1", status="done"))
        with pytest.raises(TerminalStateViolationError):
            await store.save(_snap("t", "s", "e1", status="other"))


@pytest.mark.unit
class TestExpireAtSort:
    """CF-SNP-015 mirror: no-expiry sorts as infinitely far (v0.1.5)."""

    async def test_query_sort_by_expire_at(self):
        from datetime import UTC, datetime, timedelta
        from orditect.protocol import Sort, SortDirection

        store = MemoryStore().snapshot
        future = datetime.now(UTC) + timedelta(hours=1)
        near = datetime.now(UTC) + timedelta(minutes=1)
        await store.save(
            _snap("a", "s", "e1").model_copy(update={"expire_at": future})
        )
        await store.save(
            _snap("b", "s", "e1").model_copy(update={"expire_at": near})
        )
        await store.save(_snap("c", "s", "e1"))

        asc = await store.query(
            sort=Sort(field="expire_at", direction=SortDirection.ASC)
        )
        assert [s.task_id for s in asc] == ["b", "a", "c"]

        desc = await store.query(
            sort=Sort(field="expire_at", direction=SortDirection.DESC)
        )
        assert [s.task_id for s in desc] == ["c", "a", "b"]