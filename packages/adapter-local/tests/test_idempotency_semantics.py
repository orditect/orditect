"""Behavioral pinning for T4 idempotency semantics (v0.1.4) on the local
adapter — reconstructed retries with identical business content dedup.
"""

from __future__ import annotations

import asyncio

import pytest

from orditect.adapter.local import LocalFileStore
from orditect.protocol import (
    AuditEvent,
    IdempotencyConflictError,
    TaskSnapshot,
    TerminalStateViolationError,
)

pytestmark = pytest.mark.unit


def _snap(tid, step, eid, status=""):
    return TaskSnapshot(
        task_id=tid, step=step, execution_id=eid, status=status
    )


class TestLocalTerminalIdempotency:
    async def test_reconstructed_identical_resave_dedups(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", "done"))
        await asyncio.sleep(0.001)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", "done"))
        rows = (tmp_path / "snapshots.ndjson").read_text().strip().splitlines()
        assert len(rows) == 1  # silent dedup: no second envelope row

    async def test_reconstructed_different_resave_conflicts(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.snapshot.save_terminal(_snap("t", "s", "e1", "done"))
        with pytest.raises(TerminalStateViolationError):
            await store.snapshot.save_terminal(_snap("t", "s", "e1", "other"))


class TestLocalAuditIdempotency:
    async def test_reconstructed_identical_append_dedups(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.audit.append(
            AuditEvent(event_id="e1", task_id="t", payload={"v": 1})
        )
        await asyncio.sleep(0.001)
        await store.audit.append(
            AuditEvent(event_id="e1", task_id="t", payload={"v": 1})
        )
        rows = (tmp_path / "audit.ndjson").read_text().strip().splitlines()
        assert len(rows) == 1

    async def test_different_payload_still_conflicts(self, tmp_path):
        store = LocalFileStore(tmp_path)
        await store.audit.append(
            AuditEvent(event_id="e1", task_id="t", payload={"v": 1})
        )
        with pytest.raises(IdempotencyConflictError):
            await store.audit.append(
                AuditEvent(event_id="e1", task_id="t", payload={"v": 2})
            )