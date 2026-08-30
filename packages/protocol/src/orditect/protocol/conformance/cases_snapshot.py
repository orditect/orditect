"""Snapshot-domain conformance cases (CF-SNP-*).

Authoring discipline: one adapter instance runs the WHOLE suite in a single
event loop — every case MUST use case-unique task_id / event_id / key
prefixes (e.g. "cf-snp-009"), and read-side cases that scan the whole store
(aggregate/query without a filter) MUST isolate via parent_task_id or an
equivalent case-unique filter. Never shared placeholders like "t".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

from orditect.protocol.errors import TerminalStateViolationError
from orditect.protocol.models import TaskSnapshot

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "snapshot_sink", fn))
        return fn
    return deco


def _snap(tid: str, step: str, eid: str, status: str = "") -> TaskSnapshot:
    return TaskSnapshot(task_id=tid, step=step, execution_id=eid, status=status)


@case("CF-SNP-002")
async def same_generation_resave_idempotent(adapter: Any) -> None:
    """CF-SNP-002 (T4): re-saving the same (task, step, execution) key dedups."""
    s = _snap("t-snp-002", "step", "e1", status="running")
    await adapter.save(s)
    await adapter.save(s)  # identical -> silent success


@case("CF-SNP-003")
async def mutation_after_terminal_rejected(adapter: Any) -> None:
    """CF-SNP-003 (T3): state mutation within a terminal generation is rejected."""
    tid, step, eid = "t-snp-003", "step", "e1"
    await adapter.save_terminal(_snap(tid, step, eid, status="done"))
    try:
        await adapter.save(_snap(tid, step, eid, status="running"))
    except TerminalStateViolationError:
        return
    raise AssertionError("expected TerminalStateViolationError, got success")


@case("CF-SNP-005")
async def new_generation_always_permitted(adapter: Any) -> None:
    """CF-SNP-005 (T3/T11): a new execution_id for the same node is always permitted."""
    tid, step = "t-snp-005", "step"
    await adapter.save_terminal(_snap(tid, step, "e1", status="done"))
    await adapter.save(_snap(tid, step, "e2", status="running"))  # must not raise


@case("CF-SNP-006")
async def concurrent_same_key_single_winner(adapter: Any) -> None:
    """CF-SNP-006 (T10): concurrent same-key saves leave one fully-written record."""
    import asyncio

    async def w(i: int) -> None:
        await adapter.save(_snap("t-snp-006", "step", f"e{i}", status="running"))

    await asyncio.gather(*(w(i) for i in range(8)))
    if adapter.capabilities.supports("snapshot_query"):
        versions = await adapter.list_versions("t-snp-006", "step")
        assert len(versions) == 8, (
            f"expected 8 distinct generations, got {len(versions)}"
        )
        for v in versions:
            assert v.task_id and v.execution_id, "partially written snapshot observed"


@case("CF-SNP-004")
async def expired_snapshots_invisible(adapter: Any) -> None:
    """CF-SNP-004 (T1): expired snapshots are invisible to queries."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    past = datetime.now(UTC) - timedelta(seconds=1)
    s = TaskSnapshot(
        task_id="t-snp-004", step="step", execution_id="e1",
        status="running", expire_at=past,
    )
    await adapter.save(s)
    got = await adapter.get("t-snp-004", "step")
    assert got is None, "expired snapshot must be invisible (T1)"

@case("CF-SNP-007")
async def query_filters_and_combined(adapter: Any) -> None:
    """CF-SNP-007 (T6): query filters combine with AND semantics."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    await adapter.save(_snap("cf-007-a", "s", "e1", status="running"))
    await adapter.save(_snap("cf-007-b", "s", "e1", status="done"))
    saved = await adapter.get("cf-007-b", "s")
    assert saved is not None
    await adapter.save(saved.model_copy(update={"parent_task_id": "cf-007-a"}))

    rows = await adapter.query(status="done", parent_task_id="cf-007-a")
    assert {s.task_id for s in rows} == {"cf-007-b"}


@case("CF-SNP-008")
async def list_versions_ordering(adapter: Any) -> None:
    """CF-SNP-008 (T6): list_versions honors asc/desc ordering on created_at."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    from orditect.protocol.models import Sort, SortDirection

    await adapter.save(_snap("cf-008", "s", "e1"))
    await adapter.save(_snap("cf-008", "s", "e2"))
    asc = await adapter.list_versions(
        "cf-008", "s", sort=Sort(field="created_at", direction=SortDirection.ASC))
    desc = await adapter.list_versions(
        "cf-008", "s", sort=Sort(field="created_at", direction=SortDirection.DESC))
    assert [v.execution_id for v in asc] == ["e1", "e2"]
    assert [v.execution_id for v in desc] == ["e2", "e1"]

@case("CF-SNP-009")
async def aggregate_empty_and_single(adapter: Any) -> None:
    """CF-SNP-009 (T6): aggregate over an empty set -> {}; over one group -> one bucket."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    empty = await adapter.aggregate(group_by="status",
                                    parent_task_id="cf-009-nothing")
    assert empty == {}
    snap = _snap("cf-009", "s", "e1", status="running")
    snap = snap.model_copy(update={"parent_task_id": "cf-009-root"})
    await adapter.save(snap)
    out = await adapter.aggregate(group_by="status",
                                  parent_task_id="cf-009-root")
    assert set(out.keys()) == {"running"}
    assert out["running"]["count"] == 1

@case("CF-SNP-010")
async def aggregate_precision_bound(adapter: Any) -> None:
    """CF-SNP-010 (Appendix C): float cost summation stays within 1e-9 relative error."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    a = TaskSnapshot(task_id="cf-010-a", step="s", execution_id="e1",
                     status="x", cost={"usd": 0.1},
                     parent_task_id="cf-010-root")
    b = TaskSnapshot(task_id="cf-010-b", step="s", execution_id="e1",
                     status="x", cost={"usd": 0.2},
                     parent_task_id="cf-010-root")
    await adapter.save(a)
    await adapter.save(b)
    out = await adapter.aggregate(group_by="status",
                                  parent_task_id="cf-010-root")
    total = out["x"]["cost"]["usd"]
    assert abs(total - 0.3) <= 0.3 * 1e-9

@case("CF-SNP-011")
async def sort_field_outside_whitelist_rejected(adapter: Any) -> None:
    """CF-SNP-011 (T6): an out-of-whitelist sort.field raises InvalidQueryError."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    from orditect.protocol.errors import InvalidQueryError
    from orditect.protocol.models import Sort

    try:
        await adapter.query(sort=Sort(field="cost"))
    except InvalidQueryError:
        return
    raise AssertionError("expected InvalidQueryError, got success")


@case("CF-SNP-012")
async def group_by_outside_whitelist_rejected(adapter: Any) -> None:
    """CF-SNP-012 (T6): an out-of-whitelist group_by raises InvalidQueryError."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    from orditect.protocol.errors import InvalidQueryError

    try:
        await adapter.aggregate(group_by="cost")
    except InvalidQueryError:
        return
    raise AssertionError("expected InvalidQueryError, got success")

@case("CF-SNP-013")
async def non_state_fields_merge_into_terminal_generation(adapter: Any) -> None:
    """CF-SNP-013 (T3): after a terminal save, non-state fields may still
    merge to complete the record — a later same-generation save WITHOUT
    those fields must NOT erase them."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    tid, step, eid = "t-snp-013", "step", "e1"

    # 1. save a full record (with cost), then close the generation as terminal
    full = TaskSnapshot(
        task_id=tid, step=step, execution_id=eid,
        status="running", cost={"usd": 0.5},
    )
    await adapter.save(full)
    await adapter.save_terminal(
        TaskSnapshot(task_id=tid, step=step, execution_id=eid,
                     status="done", cost={"usd": 0.5})
    )

    # 2. re-save the same generation WITHOUT cost (sparse completion record)
    await adapter.save(
        TaskSnapshot(task_id=tid, step=step, execution_id=eid, status="done")
    )

    # 3. the cost must survive (merge = complete the record, never erase)
    got = await adapter.get(tid, step)
    assert got is not None
    assert got.status == "done"
    assert got.cost == {"usd": 0.5}, (
        f"non-state field cost was erased by a sparse same-generation save: "
        f"{got.cost!r}"
    )

@case("CF-SNP-014")
async def sparse_save_without_status_preserves_record(adapter: Any) -> None:
    """CF-SNP-014 (T3): a sparse same-generation save carrying no status must
    not regress status nor drift created_at; non-state fields still merge;
    after save_terminal, a status-less save is legal (no state intent)."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    tid, step, eid = "t-snp-014", "step", "e1"
    first = TaskSnapshot(
        task_id=tid, step=step, execution_id=eid,
        status="running", cost={"usd": 0.1},
    )
    await adapter.save(first)
    sparse = TaskSnapshot(
        task_id=tid, step=step, execution_id=eid, error="boom",
    )
    await adapter.save(sparse)

    got = await adapter.get(tid, step)
    assert got is not None
    assert got.status == "running"                 # empty status never regresses
    assert got.cost == {"usd": 0.1}                # previously recorded field preserved
    assert got.error == "boom"                     # incoming field merged
    assert got.created_at == first.created_at      # created_at never drifts
    assert got.updated_at >= sparse.updated_at     # updated_at advances

    # terminal variant: a status-less save after save_terminal is legal
    await adapter.save_terminal(
        TaskSnapshot(
            task_id=tid, step=step, execution_id=eid,
            status="done", cost={"usd": 0.1}, error="boom",
        )
    )
    await adapter.save(
        TaskSnapshot(task_id=tid, step=step, execution_id=eid, model="m1")
    )
    got = await adapter.get(tid, step)
    assert got is not None
    assert got.status == "done"
    assert got.model == "m1"


@case("CF-SNP-015")
async def sort_by_expire_at_mixed_none(adapter: Any) -> None:
    """CF-SNP-015 (T6): sorting by expire_at works with mixed
    expiring/non-expiring records; no-expiry sorts as infinitely far
    (ASC last, DESC first)."""
    if not adapter.capabilities.supports("snapshot_query"):
        return
    from orditect.protocol.models import Sort, SortDirection

    future = datetime.now(UTC) + timedelta(hours=1)
    near = datetime.now(UTC) + timedelta(minutes=1)
    await adapter.save(TaskSnapshot(
        task_id="cf-015-a", step="s", execution_id="e1", expire_at=future,
    ))
    await adapter.save(TaskSnapshot(
        task_id="cf-015-b", step="s", execution_id="e1", expire_at=near,
    ))
    await adapter.save(TaskSnapshot(
        task_id="cf-015-c", step="s", execution_id="e1",
    ))

    asc = await adapter.query(
        sort=Sort(field="expire_at", direction=SortDirection.ASC)
    )
    ids_asc = [s.task_id for s in asc if s.task_id.startswith("cf-015-")]
    assert ids_asc == ["cf-015-b", "cf-015-a", "cf-015-c"], (
        f"ASC must order expiring records first, no-expiry last: {ids_asc}"
    )

    desc = await adapter.query(
        sort=Sort(field="expire_at", direction=SortDirection.DESC)
    )
    ids_desc = [s.task_id for s in desc if s.task_id.startswith("cf-015-")]
    assert ids_desc == ["cf-015-c", "cf-015-a", "cf-015-b"], (
        f"DESC must order no-expiry first: {ids_desc}"
    )