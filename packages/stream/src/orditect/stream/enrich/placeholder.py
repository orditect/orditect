"""Placeholder state machine + registry (single source of truth).

State transitions:
  pending --resolve--> resolved
  pending --fail-----> failed
  pending --settle timeout--> remains pending (marked as pending-at-manifest)
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from orditect.stream.events import PlaceholderState


@dataclass
class PlaceholderRecord:
    """Single placeholder record."""

    placeholder_id: str
    stream_id: str
    stage: str | None
    context_text: str
    loading_url: str
    task_ref: str = ""                    # "local:job-xxx" / "tf:task-xxx"
    state: PlaceholderState = PlaceholderState.PENDING
    url: str | None = None                # resolved 时的真实地址
    fallback_url: str | None = None       # failed 时的降级地址
    char_offset: int = 0                  # P0: 位置锚点
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    resolved_at: float | None = None
    error: str | None = None

    def elapsed(self) -> float | None:
        if self.resolved_at is None:
            return None
        return self.resolved_at - self.created_at


class PlaceholderRegistry:
    """Placeholder registry (per request)."""

    def __init__(self) -> None:
        self._records: dict[str, PlaceholderRecord] = {}
        self._events: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def register(self, record: PlaceholderRecord) -> None:
        async with self._lock:
            self._records[record.placeholder_id] = record
            self._events[record.placeholder_id] = asyncio.Event()

    def get(self, placeholder_id: str) -> PlaceholderRecord | None:
        return self._records.get(placeholder_id)

    def all(self) -> list[PlaceholderRecord]:
        return list(self._records.values())

    def pending(self) -> list[PlaceholderRecord]:
        return [r for r in self._records.values() if r.state is PlaceholderState.PENDING]

    async def mark_resolved(self, placeholder_id: str, url: str, meta: dict[str, Any] | None = None) -> PlaceholderRecord | None:
        """Mark as resolved (T7: terminal state irreversible — returns current record directly if not pending).

        Prevents "settle timeout marking failed, then late dispatch result overwrites terminal state"
        causing inconsistency between manifest snapshot and registry terminal state.
        """
        async with self._lock:
            rec = self._records.get(placeholder_id)
            if rec is None or rec.state is not PlaceholderState.PENDING:
                return rec
            rec.state = PlaceholderState.RESOLVED
            rec.url = url
            if meta:
                rec.meta.update(meta)
            rec.resolved_at = time.time()  # 终态时刻，elapsed() = 生命周期时长
            event = self._events.get(placeholder_id)
            if event:
                event.set()
            return rec

    async def mark_failed(self, placeholder_id: str, error: str, fallback_url: str | None = None) -> PlaceholderRecord | None:
        """Mark as failed (T7: terminal state irreversible, same as above)."""
        async with self._lock:
            rec = self._records.get(placeholder_id)
            if rec is None or rec.state is not PlaceholderState.PENDING:
                return rec
            rec.state = PlaceholderState.FAILED
            rec.error = error
            if fallback_url is not None:
                rec.fallback_url = fallback_url
            rec.resolved_at = time.time()
            event = self._events.get(placeholder_id)
            if event:
                event.set()
            return rec

    async def wait_one(self, placeholder_id: str, timeout: float) -> PlaceholderRecord | None:
        """Wait for a single placeholder to reach terminal state (resolved/failed), returns current record on timeout (still pending)."""
        event = self._events.get(placeholder_id)
        if event is None:
            return self._records.get(placeholder_id)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return self._records.get(placeholder_id)

    async def wait_all(self, timeout: float) -> list[PlaceholderRecord]:
        """Wait for all pending placeholders to reach terminal state or timeout, returns all records."""
        pendings = self.pending()
        if not pendings:
            return self.all()
        events = [self._events[r.placeholder_id] for r in pendings if r.placeholder_id in self._events]
        if events:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(e.wait() for e in events)),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                pass
        return self.all()