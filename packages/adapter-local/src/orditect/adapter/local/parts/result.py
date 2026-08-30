"""Local-file result domain part (one JSON file per stream id, lazy expiry)."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from orditect.protocol import CapabilitySet

from orditect.adapter.local._common import atomic_write_text, read_json

#: stream_id must be a safe single path segment (no traversal).
_SAFE_ID = re.compile(r"^[A-Za-z0-9._-]+$")


class LocalResultPart:
    """Implements ResultWriter + ResultReader (lazy expiry, T1/T7)."""

    def __init__(self, root: Path) -> None:
        self._dir = root / "results"
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            result_sink=True, result_query=True, concurrency_domain="process"
        )

    def _path(self, stream_id: str) -> Path:
        if not _SAFE_ID.match(stream_id):
            raise ValueError(f"unsafe stream_id: {stream_id!r}")
        return self._dir / f"{stream_id}.json"

    async def save(
        self,
        stream_id: str,
        manifest: dict[str, Any],
        *,
        expire_at: datetime,
    ) -> None:
        doc = {"manifest": dict(manifest), "expire_at": expire_at.isoformat()}
        async with self._lock:
            atomic_write_text(
                self._path(stream_id), json.dumps(doc, ensure_ascii=False)
            )

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        doc = read_json(self._path(stream_id))
        if doc is None:
            return None
        try:
            expire_at = datetime.fromisoformat(doc["expire_at"])
        except (KeyError, ValueError):
            return None
        if datetime.now(UTC) > expire_at:  # lazy expiry (T1)
            return None
        manifest = doc.get("manifest")
        return dict(manifest) if isinstance(manifest, dict) else None