"""orditect-protocol — storage interaction contracts for the Orditect ecosystem.

This package defines the narrow protocols (interfaces) and data models that
decouple the Orditect frameworks (core / flow / stream) from concrete storage
backends (PostgreSQL, MinIO, Milvus, memory, etc.).

Boundary discipline:
- No storage implementation in this package (contract only).
- No Redis dialect (governance hot path stays in orditect-core, pinned to Redis).
- No business semantics (no WHERE-style query DSL, only mechanism fields).
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.domains import (
    AuditReader,
    AuditWriter,
    ContentReader,
    ContentWriter,
    DependencyReader,
    DependencyWriter,
    ResultReader,
    ResultWriter,
    SnapshotReader,
    SnapshotWriter,
)
from orditect.protocol.errors import (
    ContractError,
    ContentNotFoundError,
    IdempotencyConflictError,
    InvalidQueryError,
    SnapshotNotFoundError,
    TerminalStateViolationError,
    UnsupportedCapabilityError,
)
from orditect.protocol.models import (
    AuditEvent,
    DependencyEdge,
    DependencyGraph,
    Page,
    Sort,
    SortDirection,
    TaskPointer,
    TaskSnapshot,
    TimeRange,
)
from orditect.protocol.rules import run_rules

try:
    __version__ = _pkg_version("orditect-protocol")
except PackageNotFoundError:
    # Development environment without installation (bare sys.path import)
    __version__ = "0.0.0.dev0"

__all__ = [
    "__version__",
    # errors
    "ContractError",
    "ContentNotFoundError",
    "IdempotencyConflictError",
    "InvalidQueryError",
    "SnapshotNotFoundError",
    "TerminalStateViolationError",
    "UnsupportedCapabilityError",
    # capabilities
    "CapabilitySet",
    # models
    "AuditEvent",
    "DependencyEdge",
    "DependencyGraph",
    "Page",
    "Sort",
    "SortDirection",
    "TaskPointer",
    "TaskSnapshot",
    "TimeRange",
    # domain protocols
    "AuditWriter",
    "AuditReader",
    "ContentWriter",
    "ContentReader",
    "DependencyWriter",
    "DependencyReader",
    "ResultWriter",
    "ResultReader",
    "SnapshotWriter",
    "SnapshotReader",
    "run_rules",
]