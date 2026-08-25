"""In-memory audit domain part."""

from __future__ import annotations

import asyncio

from orditect.protocol import (
    AuditEvent,
    CapabilitySet,
    IdempotencyConflictError,
    Page,
    Sort,
    SortDirection,
    TimeRange,
)


class MemoryAuditPart:
    """Implements AuditWriter + AuditReader (append-only, idempotent)."""

    def __init__(self) -> None:
        self._events: dict[str, AuditEvent] = {}
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(audit_sink=True, audit_query=True)

    async def append(self, event: AuditEvent) -> None:
        async with self._lock:
            existing = self._events.get(event.event_id)
            if existing is None:
                self._events[event.event_id] = event
                return
            # Same key: identical payload -> silent dedup; different -> conflict (T4).
            if existing.model_dump() != event.model_dump():
                raise IdempotencyConflictError(
                    f"event_id {event.event_id!r} reused with a different payload"
                )

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
        rows = list(self._events.values())
        if task_id is not None:
            rows = [e for e in rows if e.task_id == task_id]
        if scope is not None:
            rows = [e for e in rows if e.scope == scope]
        if event_type is not None:
            rows = [e for e in rows if e.event_type == event_type]
        if time_range is not None:
            if time_range.start is not None:
                rows = [e for e in rows if e.timestamp >= time_range.start]
            if time_range.end is not None:
                rows = [e for e in rows if e.timestamp < time_range.end]

        sort = sort or Sort()
        reverse = sort.direction is SortDirection.DESC
        rows.sort(key=lambda e: getattr(e, sort.field, e.timestamp), reverse=reverse)

        page = page or Page()
        return rows[page.offset: page.offset + page.limit]