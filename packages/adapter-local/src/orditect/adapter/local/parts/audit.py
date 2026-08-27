"""Local-file audit domain part (append-only ndjson envelope stream)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from orditect.protocol import (
    AuditEvent,
    CapabilitySet,
    IdempotencyConflictError,
    InvalidQueryError,
    Page,
    Sort,
    SortDirection,
    TimeRange,
)
from orditect.protocol.mechanism import SORT_FIELDS

from orditect.adapter.local._common import append_envelope, iter_envelopes, parse_dt


class LocalAuditPart:
    """Implements AuditWriter + AuditReader over audit.ndjson (T4 idempotent)."""

    def __init__(self, root: Path) -> None:
        self._file = root / "audit.ndjson"
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            audit_sink=True, audit_query=True, concurrency_domain="process"
        )

    async def append(self, event: AuditEvent) -> None:
        async with self._lock:
            existing = self._find(event.event_id)
            if existing is not None:
                # Same key: identical payload -> silent dedup; different -> conflict (T4).
                if existing != event.to_payload():
                    raise IdempotencyConflictError(
                        f"event_id {event.event_id!r} reused with a different payload"
                    )
                return
            append_envelope(self._file, "append", event.to_payload())

    def _find(self, event_id: str) -> dict | None:
        for row in iter_envelopes(self._file):
            if row["data"].get("event_id") == event_id:
                return row["data"]
        return None

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
        rows = [row["data"] for row in iter_envelopes(self._file)]
        if task_id is not None:
            rows = [e for e in rows if e.get("task_id") == task_id]
        if scope is not None:
            rows = [e for e in rows if e.get("scope") == scope]
        if event_type is not None:
            rows = [e for e in rows if e.get("event_type") == event_type]
        if time_range is not None:
            if time_range.start is not None:
                rows = [
                    e
                    for e in rows
                    if (dt := parse_dt(e.get("created_at"))) is not None
                    and dt >= time_range.start
                ]
            if time_range.end is not None:
                rows = [
                    e
                    for e in rows
                    if (dt := parse_dt(e.get("created_at"))) is not None
                    and dt < time_range.end
                ]

        sort = sort or Sort()
        if sort.field not in SORT_FIELDS["audit"]:
            raise InvalidQueryError(
                f"sort.field {sort.field!r} outside the audit mechanism "
                f"whitelist: {sorted(SORT_FIELDS['audit'])}"
            )
        reverse = sort.direction is SortDirection.DESC
        rows.sort(key=lambda e: str(e.get(sort.field, "")), reverse=reverse)

        page = page or Page()
        return [
            AuditEvent.model_validate(e)
            for e in rows[page.offset: page.offset + page.limit]
        ]