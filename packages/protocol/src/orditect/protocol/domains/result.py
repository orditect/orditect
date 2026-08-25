"""Result domain: persistence of stream results / manifests with true TTL.

Origin: distilled from the stream ResultStoreProtocol after the v0.3.2
true-TTL fix (save carries a real expiry; get returns None once expired).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from orditect.protocol.capabilities import CapabilitySet


@runtime_checkable
class ResultWriter(Protocol):
    """Write side of the result domain.

    Capability half-domain: result_sink.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def save(
        self,
        stream_id: str,
        manifest: dict[str, Any],
        *,
        expire_at: datetime,
    ) -> None:
        """Save a manifest under a stream id with an absolute expiry instant.

        Semantics: upserts the manifest for `stream_id`. `expire_at` is an
        absolute, timezone-aware UTC instant (terms T1, T7); once the instant
        passes, the record is invisible to readers regardless of whether the
        implementation physically removed it (lazy expiry, T1).

        Idempotency / concurrency: `stream_id` is the idempotency key (T4).
        Re-saving the same stream_id with an identical manifest is a silent
        success; with a different manifest it replaces the record (last
        writer wins) and must never leave a partially-written record
        observable (term T10).

        Raises:
            UnsupportedCapabilityError: result_sink not declared (T8).
            ContractError: any other failure (T9).
        """
        ...


@runtime_checkable
class ResultReader(Protocol):
    """Read side of the result domain.

    Capability half-domain: result_query.
    """

    @property
    def capabilities(self) -> CapabilitySet:
        """Declared capability set (term T8)."""
        ...

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        """Read a manifest by stream id.

        Semantics: returns the manifest, or None when the record does not
        exist or has expired (lazy expiry — the reader filters on expire_at,
        term T1; T7 clock discipline applies to the comparison).

        Raises:
            UnsupportedCapabilityError: result_query not declared (T8).
            ContractError: any other failure (T9). A missing/expired record
                is NOT an error — it returns None.
        """
        ...