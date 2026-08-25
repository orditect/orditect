"""In-memory result domain part."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from orditect.protocol import CapabilitySet


class MemoryResultPart:
    """Implements ResultWriter + ResultReader (lazy expiry, T1/T7)."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[dict[str, Any], datetime]] = {}
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(result_sink=True, result_query=True)

    async def save(
        self,
        stream_id: str,
        manifest: dict[str, Any],
        *,
        expire_at: datetime,
    ) -> None:
        async with self._lock:
            self._data[stream_id] = (dict(manifest), expire_at)

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        item = self._data.get(stream_id)
        if item is None:
            return None
        manifest, expire_at = item
        if datetime.now(UTC) > expire_at:  # lazy expiry (T1)
            return None
        return dict(manifest)