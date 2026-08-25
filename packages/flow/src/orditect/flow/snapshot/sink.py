"""Snapshot sink protocol adapter (flow side).

Flow writes snapshots to orditect.protocol.domains.snapshot.SnapshotWriter.
The sink here is a thin adapter: it assembles TaskSnapshot from the
executor's lifecycle context and delegates to the protocol writer.

Discipline (terms):
- T9  observation non-blocking: all sink writes are wrapped by the caller
  (executor) in try/except; a sink failure never blocks task execution.
- T11 execution identity alignment: execution_id is read from the core hot
  record (task record), assigned at initialize (C3.5) and advanced by reopen.
- T3  terminal irreversibility: terminal writes use save_terminal; non-terminal
  writes use save.
"""
from __future__ import annotations

import logging
from typing import Any, Optional, Protocol

from orditect.protocol import TaskSnapshot

logger = logging.getLogger(__name__)


class SnapshotSink(Protocol):
    """Flow-side snapshot sink (duck-typed over protocol SnapshotWriter)."""

    async def write(
        self,
        *,
        task_id: str,
        execution_id: str,
        parent_task_id: Optional[str],
        status: str,
        terminal: bool,
        cost: Optional[dict[str, float]] = None,
        error: Optional[str] = None,
        output_pointer: Any = None,
        expire_at: Any = None,
    ) -> None:
        """Write one snapshot for the single-step execution (step="execute")."""
        ...


class NullSnapshotSink:
    """Default no-op sink: zero cost, zero behavior change."""

    async def write(self, **kwargs: Any) -> None:
        return None


class ProtocolSnapshotSink:
    """Sink backed by a protocol SnapshotWriter (the real injection point)."""

    def __init__(self, writer: Any):  # writer: protocol SnapshotWriter
        self._writer = writer

    async def write(
        self,
        *,
        task_id: str,
        execution_id: str,
        parent_task_id: Optional[str],
        status: str,
        terminal: bool,
        cost: Optional[dict[str, float]] = None,
        error: Optional[str] = None,
        output_pointer: Any = None,
        expire_at: Any = None,
    ) -> None:
        snap = TaskSnapshot(
            task_id=task_id,
            step="execute",  # single-step task convention (decision A)
            execution_id=execution_id,
            parent_task_id=parent_task_id,
            status=status,
            cost=cost,
            error=error,
            output_pointer=output_pointer,
            expire_at=expire_at,
        )
        if terminal:
            await self._writer.save_terminal(snap)
        else:
            await self._writer.save(snap)

class SnapshotQuery(Protocol):
    """Read side for reuse short-circuit (duck-typed over protocol SnapshotReader).

    Used by the executor to decide whether a node's latest generation already
    succeeded — if so, the result is reused from the core hot record instead
    of re-executing (option B: no pointer-ization forced).
    """

    async def latest_status(self, task_id: str, step: str = "execute") -> Optional[str]:
        """Return the latest generation's status word, or None if no snapshot."""
        ...


class NullSnapshotQuery:
    """Default: no snapshot data — never short-circuits (zero behavior change)."""

    async def latest_status(self, task_id: str, step: str = "execute") -> Optional[str]:
        return None


class ProtocolSnapshotQuery:
    """Query backed by a protocol SnapshotReader."""

    def __init__(self, reader: Any):  # reader: protocol SnapshotReader
        self._reader = reader

    async def latest_status(self, task_id: str, step: str = "execute") -> Optional[str]:
        snap = await self._reader.get(task_id, step)
        return snap.status if snap is not None else None