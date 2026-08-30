"""Behavioral tests beyond the conformance suite: stream folding, T3/T4/T1
edge semantics, atomicity, and the trace-bundle data form."""

import json

import pytest

from orditect.adapter.local import LocalFileStore
from orditect.protocol import (
    AuditEvent,
    DependencyEdge,
    IdempotencyConflictError,
    TaskSnapshot,
    TerminalStateViolationError,
)

pytestmark = pytest.mark.unit


def _snap(tid, step, eid, status="", parent=None):
    return TaskSnapshot(
        task_id=tid, step=step, execution_id=eid,
        parent_task_id=parent, status=status,
    )


class TestTerminalFolding:
    async def test_save_terminal_then_conflicting_resave_rejected(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", status="done"))
        with pytest.raises(TerminalStateViolationError):
            await store.snapshot.save_terminal(_snap("t", "s", "e1", status="other"))

    async def test_save_terminal_identical_resave_dedups(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", status="done"))
        await store.snapshot.save_terminal(_snap("t", "s", "e1", status="done"))
        rows = (tmp_path / "snapshots.ndjson").read_text().strip().splitlines()
        assert len(rows) == 1  # T4: identical re-save is a silent dedup

    async def test_non_state_merge_into_terminal_generation(self, tmp_path):
        """T3: status never merges; non-state fields may complete the record."""
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", status="done"))
        merged = _snap("t", "s", "e1", status="done").model_copy(
            update={"cost": {"usd": 0.1}}
        )
        await store.snapshot.save(merged)  # same status, new cost: allowed
        got = await store.snapshot.get("t", "s")
        assert got is not None
        assert got.status == "done"
        assert got.cost == {"usd": 0.1}

    async def test_sparse_resave_preserves_cost(self, tmp_path):
        """CF-SNP-013 mirror (v0.1.4): sparse same-generation save must not
        erase previously recorded non-state fields."""
        store = LocalFileStore(tmp_path)
        await store.snapshot.save(
            _snap("t", "s", "e1", status="running").model_copy(
                update={"cost": {"usd": 0.5}}
            )
        )
        await store.snapshot.save_terminal(
            _snap("t", "s", "e1", status="done").model_copy(
                update={"cost": {"usd": 0.5}}
            )
        )
        await store.snapshot.save(
            _snap("t", "s", "e1", status="done")
        )  # sparse

        got = await store.snapshot.get("t", "s")
        assert got is not None
        assert got.cost == {"usd": 0.5}

    async def test_sparse_save_without_status_preserves_record(self, tmp_path):
        """CF-SNP-014 mirror (v0.1.5): empty-status sparse saves never
        regress status, never drift created_at, still merge fields."""
        store = LocalFileStore(tmp_path)
        first = _snap("t", "s", "e1", status="running").model_copy(
            update={"cost": {"usd": 0.1}}
        )
        await store.snapshot.save(first)
        sparse = _snap("t", "s", "e1").model_copy(
            update={"status": "", "error": "boom"}
        )
        await store.snapshot.save(sparse)

        got = await store.snapshot.get("t", "s")
        assert got is not None
        assert got.status == "running"
        assert got.cost == {"usd": 0.1}
        assert got.error == "boom"
        assert got.created_at == first.created_at

    async def test_statusless_save_after_terminal_is_legal(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(
            _snap("t", "s", "e1", status="done").model_copy(
                update={"cost": {"usd": 0.1}}
            )
        )
        sparse = _snap("t", "s", "e1").model_copy(
            update={"status": "", "model": "m1"}
        )
        await store.snapshot.save(sparse)

        got = await store.snapshot.get("t", "s")
        assert got is not None
        assert got.status == "done"
        assert got.model == "m1"

class TestIdempotency:
    async def test_audit_conflict(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.audit.append(
            AuditEvent(event_id="e1", task_id="t", payload={"v": 1})
        )
        with pytest.raises(IdempotencyConflictError):
            await store.audit.append(
                AuditEvent(event_id="e1", task_id="t", payload={"v": 2})
            )

    async def test_edge_conflict(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.dependency.write_dependency(
            DependencyEdge(child_id="c", parent_id="p", is_primary=True)
        )
        with pytest.raises(IdempotencyConflictError):
            await store.dependency.write_dependency(
                DependencyEdge(child_id="c", parent_id="p", is_primary=False)
            )


class TestAtomicWrite:
    async def test_no_tmp_residue_after_put(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.result.save("s1", {"k": "v"}, expire_at=_far_future())
        tmp_files = list(tmp_path.rglob("*.json.*"))
        assert tmp_files == []  # tmp files must be renamed away, never left


class TestTraceBundleForm:
    async def test_bundle_rows_are_protocol_consumable(self, tmp_path):
        """The directory layout doubles as the trace-bundle form: ndjson
        envelope rows readable by run_rules without orditect internals."""
        from orditect.protocol.rules import run_rules

        store = LocalFileStore(tmp_path)
        await store.snapshot.save(_snap("t1", "execute", "e1", "done"))
        await store.snapshot.save_terminal(_snap("t1", "execute", "e1", "done"))
        await store.audit.append(
            AuditEvent(event_id="ev1", task_id="t1", payload={"a": 1})
        )
        await store.dependency.write_dependency(
            DependencyEdge(child_id="t1", parent_id="root")
        )

        lines: list[dict] = []
        for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
            path = tmp_path / name
            for raw in path.read_text(encoding="utf-8").splitlines():
                lines.append(json.loads(raw))

        report = run_rules(lines)
        assert report.ok, report.summary()

class TestExpireAtSort:
    """CF-SNP-015 mirror (v0.1.5): no-expiry sorts as infinitely far."""

    async def test_query_sort_by_expire_at(self, tmp_path):
        from datetime import UTC, datetime, timedelta
        from orditect.protocol import Sort, SortDirection

        store = LocalFileStore(tmp_path)
        future = datetime.now(UTC) + timedelta(hours=1)
        near = datetime.now(UTC) + timedelta(minutes=1)
        await store.snapshot.save(
            _snap("a", "s", "e1").model_copy(update={"expire_at": future})
        )
        await store.snapshot.save(
            _snap("b", "s", "e1").model_copy(update={"expire_at": near})
        )
        await store.snapshot.save(_snap("c", "s", "e1"))

        asc = await store.snapshot.query(
            sort=Sort(field="expire_at", direction=SortDirection.ASC)
        )
        assert [s.task_id for s in asc] == ["b", "a", "c"]

        desc = await store.snapshot.query(
            sort=Sort(field="expire_at", direction=SortDirection.DESC)
        )
        assert [s.task_id for s in desc] == ["c", "a", "b"]

def _far_future():
    from datetime import UTC, datetime, timedelta

    return datetime.now(UTC) + timedelta(hours=1)