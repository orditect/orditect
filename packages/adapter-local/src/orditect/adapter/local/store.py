"""LocalFileStore: facade composing the five per-domain parts.

The directory layout doubles as the trace-bundle data form (ndjson
envelope rows + JSON payloads), readable by any consumer without importing
orditect.
"""

from __future__ import annotations

from pathlib import Path

from orditect.adapter.local.parts.audit import LocalAuditPart
from orditect.adapter.local.parts.content import LocalContentPart
from orditect.adapter.local.parts.dependency import LocalDependencyPart
from orditect.adapter.local.parts.result import LocalResultPart
from orditect.adapter.local.parts.snapshot import LocalSnapshotPart


class LocalFileStore:
    """Local-file storage composing all five protocol domains.

    Each part is independently usable and exposes its own CapabilitySet
    (concurrency_domain="process"), mirroring the memory adapter's
    per-domain-part structure.
    """

    def __init__(self, root: str | Path) -> None:
        root_path = Path(root)
        self.root = root_path
        self.content = LocalContentPart(root_path)
        self.audit = LocalAuditPart(root_path)
        self.result = LocalResultPart(root_path)
        self.snapshot = LocalSnapshotPart(root_path)
        self.dependency = LocalDependencyPart(root_path)