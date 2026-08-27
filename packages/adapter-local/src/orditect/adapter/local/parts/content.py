"""Local-file content domain part (content-addressed blobs)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

from orditect.protocol import CapabilitySet, ContentNotFoundError, TaskPointer

from orditect.adapter.local._common import (
    atomic_write_bytes,
    atomic_write_text,
    read_json,
)


class LocalContentPart:
    """Implements ContentWriter + ContentReader over content-addressed files."""

    def __init__(self, root: Path) -> None:
        self._root = root / "content" / "sha256"
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            content_sink=True, content_query=True, concurrency_domain="process"
        )

    def _paths(self, digest: str) -> tuple[Path, Path]:
        return (
            self._root / digest[:2] / digest,
            self._root / digest[:2] / f"{digest}.meta.json",
        )

    def _key(self, digest: str) -> str:
        return f"sha256/{digest[:2]}/{digest}"

    async def put(
        self,
        content: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskPointer:
        digest = hashlib.sha256(bytes(content)).hexdigest()
        blob_path, meta_path = self._paths(digest)
        meta = dict(metadata or {})
        if content_type is not None:
            meta["content_type"] = content_type
        async with self._lock:
            # Content-addressed writes are inherently idempotent: identical
            # content maps to the same key (T4); concurrent puts of identical
            # content converge on the same blob (T10).
            if not blob_path.is_file():
                atomic_write_bytes(blob_path, bytes(content))
            if meta or not meta_path.is_file():
                atomic_write_text(
                    meta_path, json.dumps(meta, ensure_ascii=False)
                )
        return TaskPointer(
            backend="localfile", key=self._key(digest), metadata=meta or None
        )

    def _digest_of(self, pointer: TaskPointer) -> str:
        # Key form: "sha256/<aa>/<digest>"; backend prefix is irrelevant here.
        return pointer.key.rsplit("/", 1)[-1]

    async def delete(self, pointer: TaskPointer) -> bool:
        blob_path, meta_path = self._paths(self._digest_of(pointer))
        existed = blob_path.is_file()
        async with self._lock:
            blob_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
        return existed

    async def get(self, pointer: TaskPointer) -> bytes:
        blob_path, _ = self._paths(self._digest_of(pointer))
        try:
            return blob_path.read_bytes()
        except OSError:
            raise ContentNotFoundError(pointer.key) from None

    async def exists(self, pointer: TaskPointer) -> bool:
        blob_path, _ = self._paths(self._digest_of(pointer))
        return blob_path.is_file()

    async def get_metadata(self, pointer: TaskPointer) -> dict[str, Any]:
        blob_path, meta_path = self._paths(self._digest_of(pointer))
        if not blob_path.is_file():
            raise ContentNotFoundError(pointer.key)
        return read_json(meta_path) or {}