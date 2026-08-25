"""Audit domain: append-only, idempotent event log with mechanism-field query.

Origin: generalized from flow's BudgetAuditSink (governor/budget.py). The
dual-habitat idempotency key discipline (hot path quota + cold path audit
dedup by the same key) is preserved: `event_id` is that key.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.models import AuditEvent, Page, Sort, TimeRange


@runtime_checkable
class AuditWriter(Protocol):
    """Write side of the audit domain.

    Capability half-domain: audit_sink.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def append(self, event: AuditEvent) -> None:
        """Append one event to the log (append-only, idempotent).

        Semantics: appends the event. The log is append-only — no method in
        this contract mutates or removes previously appended events.

        Idempotency / concurrency: `event.event_id` is the idempotency key
        (term T4). Re-appending the same event_id with an identical payload
        is a silent success (dedup). Re-using the same event_id with a
        different payload raises IdempotencyConflictError. Concurrent appends
        with the same event_id must leave exactly one record (term T10).

        Raises:
            IdempotencyConflictError: same event_id, different payload (T4).
            UnsupportedCapabilityError: audit_sink not declared (T8).
            ContractError: any other failure (T9 — only ContractError
                subclasses may escape this method).
        """
        ...


@runtime_checkable
class AuditReader(Protocol):
    """Read side of the audit domain.

    Capability half-domain: audit_query.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def query(
        self,
        *,
        task_id: str | None = None,
        scope: str | None = None,
        event_type: str | None = None,
        time_range: TimeRange | None = None,
        page: Page | None = None,
        sort: Sort | None = None,
    ) -> list[AuditEvent]:
        """Query audit events by mechanism fields only.

        Semantics: returns events matching all provided filters (AND
        combination). All filter fields are mechanism fields; business-predicate
        filtering on payload content is out of contract scope (iron rule).
        Missing filters match everything. Default ordering is by timestamp
        descending (see Sort model default).

        Raises:
            UnsupportedCapabilityError: audit_query not declared (T8).
            ContractError: any other failure (T9).
        """
        ...