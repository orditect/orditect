"""Protocol-backed result store adapter (S2).

Thin adapter relocating the stream ResultStoreProtocol onto the
orditect-protocol result domain (ResultWriter/ResultReader), while keeping
the stream-facing signature unchanged (ttl seconds) for backward
compatibility.

Mapping discipline:
- ttl (seconds, relative)  ->  expire_at (absolute, timezone-aware UTC)
  computed at save time (terms T1/T7: absolute instant on the contract).
- get semantics unchanged: returns None once expired (lazy expiry, T1).

Vocabulary / modality neutrality: the manifest is an opaque dict to this
adapter; no field is interpreted.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from orditect.stream.protocols import ResultStoreProtocol


class ProtocolResultStore(ResultStoreProtocol):
    """ResultStoreProtocol backed by protocol result-domain parts.

    Args:
        writer: protocol ResultWriter (e.g. orditect.adapter.memory
            MemoryResultPart, or a commercial PG adapter's result part).
        reader: protocol ResultReader. May be the same object as writer when
            the backend composes both half-domains.
    """

    def __init__(self, writer: Any, reader: Any):
        self._writer = writer
        self._reader = reader

    async def save(self, stream_id: str, manifest: dict[str, Any], ttl: int) -> None:
        """Save manifest (ttl seconds converted to absolute expire_at, T1/T7)."""
        expire_at = (
            datetime.now(UTC) + timedelta(seconds=ttl)
            if ttl > 0
            else datetime.max.replace(tzinfo=UTC)  # no expiry -> far future
        )
        await self._writer.save(stream_id, manifest, expire_at=expire_at)

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        """Read manifest (None when missing or expired — lazy expiry, T1)."""
        return await self._reader.get(stream_id)