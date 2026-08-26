"""Audit-domain conformance cases (CF-AUD-*).

Authoring discipline: one adapter instance runs the WHOLE suite in a single
event loop — every case MUST use case-unique task_id / event_id / key
prefixes (e.g. "cf-aud-003-t"), never shared placeholders like "t".
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from orditect.protocol.errors import IdempotencyConflictError
from orditect.protocol.models import AuditEvent

CaseFn = Callable[[Any], Awaitable[None]]
CASES: list[tuple[str, str, CaseFn]] = []


def case(case_id: str):
    def deco(fn: CaseFn) -> CaseFn:
        CASES.append((case_id, "audit_sink", fn))
        return fn
    return deco


@case("CF-AUD-001")
async def duplicate_append_deduplicated(adapter: Any) -> None:
    """CF-AUD-001 (T4): re-appending the same event_id with identical payload dedups."""
    ev = AuditEvent(event_id="cf-aud-001", task_id="t", payload={"a": 1})
    await adapter.append(ev)
    await adapter.append(ev)  # same key, same payload -> silent success
    # If the adapter also declares audit_query, verify exactly one record.
    if adapter.capabilities.supports("audit_query"):
        rows = await adapter.query(task_id="t")
        assert len(rows) == 1, f"expected 1 record, got {len(rows)}"


@case("CF-AUD-002")
async def same_key_different_payload_conflicts(adapter: Any) -> None:
    """CF-AUD-002 (T4): same event_id with different payload raises conflict."""
    await adapter.append(AuditEvent(event_id="cf-aud-002", task_id="t", payload={"v": 1}))
    try:
        await adapter.append(
            AuditEvent(event_id="cf-aud-002", task_id="t", payload={"v": 2})
        )
    except IdempotencyConflictError:
        return
    raise AssertionError("expected IdempotencyConflictError, got success")

@case("CF-AUD-005")
async def sort_field_outside_whitelist_rejected(adapter: Any) -> None:
    """CF-AUD-005 (T6): an out-of-whitelist sort.field raises InvalidQueryError."""
    if not adapter.capabilities.supports("audit_query"):
        return
    from orditect.protocol.errors import InvalidQueryError
    from orditect.protocol.models import Sort

    try:
        await adapter.query(sort=Sort(field="payload"))
    except InvalidQueryError:
        return
    raise AssertionError("expected InvalidQueryError, got success")


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

@case("CF-AUD-003")
async def page_boundaries(adapter: Any) -> None:
    """CF-AUD-003 (T6): offset beyond the result set -> empty; limit=1 pages correctly."""
    if not adapter.capabilities.supports("audit_query"):
        return
    from orditect.protocol.models import Page

    tid = "cf-aud-003-t"  # unique task_id: one adapter instance runs the whole suite
    for i in range(3):
        await adapter.append(AuditEvent(event_id=f"cf-aud-003-{i}", task_id=tid))
    assert await adapter.query(task_id=tid, page=Page(limit=10, offset=99)) == []
    first = await adapter.query(task_id=tid, page=Page(limit=1, offset=0))
    assert len(first) == 1
    rest = await adapter.query(task_id=tid, page=Page(limit=10, offset=1))
    assert len(rest) == 2


@case("CF-AUD-004")
async def time_range_interval_semantics(adapter: Any) -> None:
    """CF-AUD-004 (T1): start inclusive, end exclusive."""
    if not adapter.capabilities.supports("audit_query"):
        return
    from datetime import UTC, datetime, timedelta
    from orditect.protocol.models import TimeRange

    tid = "cf-aud-004-t"
    now = datetime.now(UTC)
    await adapter.append(AuditEvent(event_id="cf-aud-004-a", task_id=tid,
                                    created_at=now))
    tr = TimeRange(start=now, end=now + timedelta(seconds=1))
    rows = await adapter.query(task_id=tid, time_range=tr)
    assert len(rows) == 1, "start boundary must be inclusive"
    tr2 = TimeRange(start=now - timedelta(seconds=1), end=now)
    rows2 = await adapter.query(task_id=tid, time_range=tr2)
    assert rows2 == [], "end boundary must be exclusive"