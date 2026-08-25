"""TaskPointer: the single addressing primitive for content storage.

Pointer discipline (terms):
- Content above the size threshold must be pointer-ized; records carry only
  the pointer, never the payload itself.
- Content is immutable: mutation means a new pointer, never in-place update.
- The pointer structure is backend-neutral; the `backend` field is an opaque
  identifier (e.g. "postgres", "s3", "milvus") interpreted by the adapter.
"""

from __future__ import annotations

from typing import Any

from orditect.protocol.models._base import ContractModel


class TaskPointer(ContractModel):
    """Pointer to content stored in an external backend.

    Attributes:
        backend: Opaque storage backend identifier (no business semantics).
        key: Backend-specific addressing key (path, row id, vector id, ...).
        metadata: Optional free-form metadata (content_type, size, ...).
    """

    backend: str
    key: str
    metadata: dict[str, Any] | None = None