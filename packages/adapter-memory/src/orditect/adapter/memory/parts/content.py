"""In-memory content domain part."""

from __future__ import annotations

import asyncio
from typing import Any

from orditect.protocol import CapabilitySet, ContentNotFoundError, TaskPointer


class MemoryContentPart:
    """Implements ContentWriter + ContentReader (immutable blobs)."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, dict[str, Any]]] = {}
        self._seq = 0
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(content_sink=True, content_query=True)

    async def put(
        self,
        content: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskPointer:
        async with self._lock:
            self._seq += 1
            key = f"mem://content/{self._seq}"
            meta = dict(metadata or {})
            if content_type is not None:
                meta["content_type"] = content_type
            self._data[key] = (bytes(content), meta)
        return TaskPointer(backend="memory", key=key, metadata=meta or None)

    async def delete(self, pointer: TaskPointer) -> bool:
        async with self._lock:
            return self._data.pop(pointer.key, None) is not None

    async def get(self, pointer: TaskPointer) -> bytes:
        try:
            return self._data[pointer.key][0]
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None

    async def exists(self, pointer: TaskPointer) -> bool:
        return pointer.key in self._data

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        try:
            return dict(self._data[pointer.key][1])
        except KeyError:
            raise ContentNotFoundError(pointer.key) from None