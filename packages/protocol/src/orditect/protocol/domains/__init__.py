"""Storage domain protocols (narrow interfaces, independently declared).

Each domain is split into a sink (write) protocol and a query (read)
protocol. An implementation may provide any subset; it must declare the
subset in its CapabilitySet and raise UnsupportedCapabilityError for the
rest (term T8).
"""

from orditect.protocol.domains.audit import AuditReader, AuditWriter
from orditect.protocol.domains.content import ContentReader, ContentWriter
from orditect.protocol.domains.result import ResultReader, ResultWriter
from orditect.protocol.domains.snapshot import SnapshotReader, SnapshotWriter

__all__ = [
    "AuditWriter",
    "AuditReader",
    "ContentWriter",
    "ContentReader",
    "ResultWriter",
    "ResultReader",
    "SnapshotWriter",
    "SnapshotReader",
]