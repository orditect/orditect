"""MemoryStore: facade composing the five per-domain parts."""

from __future__ import annotations

from orditect.adapter.memory.parts.audit import MemoryAuditPart
from orditect.adapter.memory.parts.content import MemoryContentPart
from orditect.adapter.memory.parts.dependency import MemoryDependencyPart
from orditect.adapter.memory.parts.result import MemoryResultPart
from orditect.adapter.memory.parts.snapshot import MemorySnapshotPart


class MemoryStore:
    """In-memory storage composing all five protocol domains.

    Each part is independently usable and exposes its own CapabilitySet.
    This per-domain-part composition is the recommended adapter structure.
    """

    def __init__(self) -> None:
        self.content = MemoryContentPart()
        self.audit = MemoryAuditPart()
        self.result = MemoryResultPart()
        self.snapshot = MemorySnapshotPart()
        self.dependency = MemoryDependencyPart()