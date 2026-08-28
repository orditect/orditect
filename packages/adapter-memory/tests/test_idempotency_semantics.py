"""Behavioral pinning for T4 idempotency semantics (v0.1.4, mechanism clock
fields excluded from comparison).

Pins the rule that a retry reconstructing a model with identical business
content but a different producer timestamp is a silent dedup, not a
conflict.
"""

from __future__ import annotations

import asyncio

import pytest

from orditect.adapter.memory import MemoryStore
from orditect.protocol import AuditEvent, TaskSnapshot

pytestmark = pytest.mark.unit


def _snap(tid, step, eid, status=""):
    return TaskSnapshot(
        task_id=tid, step=step, execution_id=eid, status=status
    )


class TestSnapshotTerminalIdempotency:
    async def test_reconstructed_identical_resave_dedups(self):
        """A terminal re-save reconstructed with the same business content
        (new created_at) is a silent dedup, not a conflict."""
        store = MemoryStore().snapshot
        await store.save_terminal(_snap("t", "s", "e1", "done"))
        await asyncio.sleep(0.001)  # ensure a different producer timestamp
        await store.save_terminal(_snap("t", "s", "e1", "done"))  # no raise

    async def test_reconstructed_different_resave_conflicts(self):
        """Different business content still conflicts (T3)."""
        from orditect.protocol import TerminalStateViolationError

        store = MemoryStore().snapshot
        await store.save_terminal(_snap("t", "s", "e1", "done"))
        with pytest.raises(TerminalStateViolationError):
            await store.save_terminal(_snap("t", "s", "e1", "other"))


class TestAuditIdempotency:
    async def test_reconstructed_identical_append_dedups(self):
        """A reconstructed event with the same business content (new
        created_at) is a silent dedup, not a conflict."""
        store = MemoryStore().audit
        await store.append(AuditEvent(event_id="e1", task_id="t", payload={"v": 1}))
        await asyncio.sleep(0.001)
        await store.append(AuditEvent(event_id="e1", task_id="t", payload={"v": 1}))
        assert len(store._events) == 1

    async def test_different_payload_still_conflicts(self):
        from orditect.protocol import IdempotencyConflictError

        store = MemoryStore().audit
        await store.append(AuditEvent(event_id="e1", task_id="t", payload={"v": 1}))
        with pytest.raises(IdempotencyConflictError):
            await store.append(
                AuditEvent(event_id="e1", task_id="t", payload={"v": 2})
            )