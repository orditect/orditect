"""Snapshot sink injection point (F2) + reuse query (F3)."""
from orditect.flow.snapshot.sink import (
    NullSnapshotQuery,
    NullSnapshotSink,
    ProtocolSnapshotQuery,
    ProtocolSnapshotSink,
    SnapshotQuery,
    SnapshotSink,
)

__all__ = [
    "SnapshotSink",
    "NullSnapshotSink",
    "ProtocolSnapshotSink",
    "SnapshotQuery",
    "NullSnapshotQuery",
    "ProtocolSnapshotQuery",
]