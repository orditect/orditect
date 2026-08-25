"""Audit-domain conformance cases (CF-AUD-*)."""

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