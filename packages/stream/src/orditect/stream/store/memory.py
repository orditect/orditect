"""MemoryResultStore: default result storage (single-instance/test).

dict + TTL lazy expiration (expired on read).
"""
from __future__ import annotations

import time
from typing import Any


class MemoryResultStore:
    """In-memory result storage (single-instance/test only).

    ⚠️ Usage boundaries: single process, single instance, development/test scenarios.
    - No cross-process consistency (for multi-instance deployment, use TaskflowResultStore/taskstore implementation)
    - No persistence (lost on process restart)
    Not for production use.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[dict[str, Any], float]] = {}  # sid -> (manifest, expire_at)

    async def save(self, stream_id: str, manifest: dict[str, Any], ttl: int) -> None:
        expire_at = time.time() + ttl if ttl > 0 else float("inf")
        self._data[stream_id] = (manifest, expire_at)

    async def get(self, stream_id: str) -> dict[str, Any] | None:
        item = self._data.get(stream_id)
        if item is None:
            return None
        manifest, expire_at = item
        if time.time() > expire_at:
            del self._data[stream_id]
            return None
        return manifest

    async def clear(self) -> None:
        self._data.clear()