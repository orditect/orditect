"""Store factory (S2: optional protocol-backend entry point).

get_default_store keeps its existing behavior (MemoryResultStore for
single-instance/test; TaskflowResultStore when taskflow storage injected).
A new optional entry point lets a protocol result-domain backend plug in via
the thin ProtocolResultStore adapter — relocation as an *access point*, not a
replacement of the built-in memory store.
"""
from __future__ import annotations

import logging
from typing import Any

from orditect.stream.protocols import ResultStoreProtocol
from orditect.stream.store.memory import MemoryResultStore

logger = logging.getLogger(__name__)


def get_default_store(storage: Any = None) -> ResultStoreProtocol:
    """Select result store implementation by injection.

    Args:
        storage: orditect-flow TaskStorageProtocol instance. None uses
            MemoryResultStore (single-instance/test scenario).

    Returns:
        TaskflowResultStore (storage injected) / MemoryResultStore
    """
    if storage is not None:
        from orditect.stream.adapters.taskflow import TaskflowResultStore
        logger.info("Using TaskflowResultStore as result store backend")
        return TaskflowResultStore(storage)
    logger.info("Using MemoryResultStore (no storage injected)")
    return MemoryResultStore()


def get_protocol_store(writer: Any, reader: Any) -> ResultStoreProtocol:
    """Build a result store over protocol result-domain parts (S2).

    Args:
        writer: protocol ResultWriter (memory / PG / ... adapter part).
        reader: protocol ResultReader.

    Returns:
        ProtocolResultStore exposing the stream-facing ResultStoreProtocol
        signature (ttl seconds), delegating to the protocol backend.

    Example:
        from orditect.adapter.memory import MemoryStore
        parts = MemoryStore()
        store = get_protocol_store(parts.result, parts.result)
    """
    from orditect.stream.store.adapter import ProtocolResultStore
    logger.info("Using protocol-backed result store (orditect-protocol result domain)")
    return ProtocolResultStore(writer, reader)