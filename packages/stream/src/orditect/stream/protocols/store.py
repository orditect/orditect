"""Result storage protocol (refetch data source) + journal reserved."""
from __future__ import annotations

from typing import Any, Protocol


class ResultStoreProtocol(Protocol):
    """Result storage: manifest persistence, for refetch / client resolver queries."""

    async def save(self, stream_id: str, manifest: dict[str, Any], ttl: int) -> None:
        """Save manifest.

        Args:
            stream_id: Stream identifier
            manifest: Data payload of stream.manifest event
            ttl: Time-to-live in seconds
        """
        ...

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        """Read manifest (returns None if not exists)."""
        ...


class JournalProtocol(Protocol):
    """Event log (reserved: true checkpoint journal).

    Currently no implementation class is provided; the protocol anchor id={stream_id}:{seq} is already in the envelope.
    """

    async def append(self, stream_id: str, seq: int, event: dict[str, Any]) -> None: ...
    async def read_from(self, stream_id: str, after_seq: int) -> list[dict[str, Any]]: ...