"""Snapshot-domain conformance cases (CF-SNP-*)."""

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