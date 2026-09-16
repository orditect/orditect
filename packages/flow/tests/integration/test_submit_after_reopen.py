"""Contract pinning for submit() after reopen_task against real TaskRedisDB.

Pins the fix for the GitHub issue "submit resets the generation chain
when resubmitting after reopen_task":
- reopen + submit: the task executes AND the generation chain is preserved.
- reopen + submit(if_not_exists=True): skipped entirely (idempotency
  contract unchanged, no double execution).
- first submission and the pinned legacy overwrite path are unaffected.

Requires a real Redis (default db15, integration-suite convention).
"""

from __future__ import annotations

import asyncio
from typing import List

import pytest

from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.flow.storage.factory import get_default_storage

pytestmark = pytest.mark.integration


class _SpyTask(BaseBackEndTask):
    def __init__(self, storage, ran: List[str]):
        super().__init__(storage)
        self._ran = ran

    async def execute(self, task_id: str, **kwargs):
        self._ran.append(task_id)
        return {"ok": True}


async def _make_orchestrator(redis_client):
    storage = get_default_storage(redis_client)
    if hasattr(storage, "connect"):
        await storage.connect()
    return storage, TaskOrchestrator(storage, governor=None)


class TestSubmitAfterReopen:
    async def test_submit_after_reopen_preserves_chain_and_executes(self, redis_client):
        storage, orchestrator = await _make_orchestrator(redis_client)

        await storage.initialize_task("t_reopen_submit", initial_status="succeeded")
        gen1 = (await storage.get_task("t_reopen_submit"))["execution_id"]

        await storage.reopen_task("t_reopen_submit")

        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_reopen_submit")

        record = await orchestrator.wait_terminal("t_reopen_submit", timeout=5.0)
        assert record["status"] == "succeeded"
        assert ran == ["t_reopen_submit"]

        rec = await storage.get_task("t_reopen_submit")
        assert rec["previous_execution_ids"] == [gen1]
        assert rec["previous_status"] == "succeeded"
        assert rec["execution_id"] != gen1

        await orchestrator.wait_all_finalized()

    async def test_if_not_exists_with_existing_pending_skips_entirely(self, redis_client):
        """Idempotency contract unchanged: an existing pending record is
        never re-executed under if_not_exists=True (no double execution)."""
        storage, orchestrator = await _make_orchestrator(redis_client)

        await storage.initialize_task("t_idem_pending", initial_status="succeeded")
        gen1 = (await storage.get_task("t_idem_pending"))["execution_id"]
        await storage.reopen_task("t_idem_pending")

        ran: List[str] = []
        task_id = await orchestrator.submit(
            _SpyTask(storage, ran), task_id="t_idem_pending", if_not_exists=True
        )
        assert task_id == "t_idem_pending"
        await asyncio.sleep(0.3)
        assert ran == []

        rec = await storage.get_task("t_idem_pending")
        assert rec["status"] == "pending"
        assert rec["previous_execution_ids"] == [gen1]

    async def test_first_submission_unaffected(self, redis_client):
        """Regression: a missing record follows the normal initialize path."""
        storage, orchestrator = await _make_orchestrator(redis_client)

        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_first")
        record = await orchestrator.wait_terminal("t_first", timeout=5.0)
        assert record["status"] == "succeeded"
        assert ran == ["t_first"]

        rec = await storage.get_task("t_first")
        assert not rec.get("previous_execution_ids")

        await orchestrator.wait_all_finalized()

    async def test_existing_non_pending_default_resets_record(self, redis_client):
        """Pinned legacy semantics: default submit on a non-pending existing
        record still resets it (the fix only diverts PENDING records)."""
        storage, orchestrator = await _make_orchestrator(redis_client)

        await storage.initialize_task("t_legacy", initial_status="succeeded")
        gen1 = (await storage.get_task("t_legacy"))["execution_id"]

        ran: List[str] = []
        await orchestrator.submit(_SpyTask(storage, ran), task_id="t_legacy")
        record = await orchestrator.wait_terminal("t_legacy", timeout=5.0)
        assert record["status"] == "succeeded"
        assert ran == ["t_legacy"]

        rec = await storage.get_task("t_legacy")
        assert not rec.get("previous_execution_ids")  # chain reset (legacy)
        assert rec["execution_id"] != gen1

        await orchestrator.wait_all_finalized()