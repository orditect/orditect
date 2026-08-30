"""Reopen primitive pinning tests (v0.1.0).

Pinning discipline: each case must fail before the fix and turn green after.
Terms verified: T3 (terminal protection), T4/T10 (idempotency/concurrency),
T11 (execution identity alignment — hot-path projection).
"""
import asyncio

import pytest

from orditect.core import (
    InvalidStatusTransferError,
    TaskNotFoundError,
    TaskRedisDB,
)


@pytest.mark.pinning
class TestReopenBasics:
    """Core reopen semantics."""

    async def test_reopen_terminal_task_produces_new_execution(self, redis_url, redis_client):
        """Terminal task reopen -> new execution_id, state reset, index migrated."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_reopen")
        await db.update_task("t_reopen", {"status": "in_progress"})
        await db.update_task("t_reopen", {"status": "completed"})

        new_eid = await db.reopen_task("t_reopen")

        assert new_eid.startswith("exec-")
        task = await db.get_task("t_reopen")
        assert task["status"] == "pending"          # state reset
        assert task["execution_id"] == new_eid      # new generation
        assert task["cancel_requested"] is False    # cancel flag cleared
        assert task["previous_status"] == "completed"

        # Status index migrated to initial state
        pending_ids = await db.list_task_ids_by_status("pending")
        assert "t_reopen" in pending_ids
        completed_ids = await db.list_task_ids_by_status("completed")
        assert "t_reopen" not in completed_ids

        await db.close()

    async def test_reopen_non_terminal_rejected(self, redis_url, redis_client):
        """Non-terminal task reopen -> explicit rejection (T3 preserved)."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_nonterm")
        await db.update_task("t_nonterm", {"status": "in_progress"})

        # Capture the initial execution_id (assigned at creation, C3.5)
        before = await db.get_task("t_nonterm")
        initial_eid = before["execution_id"]

        with pytest.raises(InvalidStatusTransferError, match="not terminal"):
            await db.reopen_task("t_nonterm")

        # State untouched: status unchanged AND execution_id NOT advanced
        task = await db.get_task("t_nonterm")
        assert task["status"] == "in_progress"
        assert task["execution_id"] == initial_eid  # rejected reopen must not advance generation

        await db.close()

    async def test_reopen_not_found(self, redis_url, redis_client):
        """Reopen non-existent task -> TaskNotFoundError."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        with pytest.raises(TaskNotFoundError):
            await db.reopen_task("ghost")

        await db.close()

    async def test_initialize_assigns_initial_execution_id(self, redis_url, redis_client):
        """C3.5: task carries an execution_id from creation (T11 hot-path)."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_init_eid")
        task = await db.get_task("t_init_eid")
        assert task["execution_id"].startswith("exec-")

        # Idempotent skip keeps the original execution_id
        first_eid = task["execution_id"]
        await db.initialize_task("t_init_eid", if_not_exists=True)
        task2 = await db.get_task("t_init_eid")
        assert task2["execution_id"] == first_eid

        await db.close()

    async def test_reopen_clears_result_and_error(self, redis_url, redis_client):
        """v0.1.5: reopen must clear the old generation's result/error —
        a new generation must not inherit the previous generation's output."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_clear")
        await db.update_task("t_clear", {"status": "in_progress"})
        await db.update_task(
            "t_clear",
            {"status": "completed", "result": {"old": 1}, "error": "x"},
        )

        await db.reopen_task("t_clear")

        task = await db.get_task("t_clear")
        assert "result" not in task
        assert "error" not in task

        await db.close()

@pytest.mark.pinning
class TestReopenConcurrency:
    """T4/T10: concurrent reopen exactly one winner."""

    async def test_concurrent_reopen_exactly_one_wins(self, redis_url, redis_client):
        """Concurrent reopen of same terminal task -> exactly one success."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_race")
        await db.update_task("t_race", {"status": "in_progress"})
        await db.update_task("t_race", {"status": "failed"})

        results = await asyncio.gather(
            db.reopen_task("t_race"),
            db.reopen_task("t_race"),
            return_exceptions=True,
        )

        successes = [r for r in results if isinstance(r, str)]
        failures = [r for r in results if isinstance(r, InvalidStatusTransferError)]

        # Exactly one winner; loser rejected as non-terminal (already reset)
        assert len(successes) == 1, f"expected 1 winner, got {successes}"
        assert len(failures) == 1

        await db.close()


@pytest.mark.pinning
class TestReopenHistory:
    """Old generation trace (T11 alignment + audit trail)."""

    async def test_reopen_preserves_old_generation_in_history(self, redis_url, redis_client):
        """Reopen appends old execution_id to previous_execution_ids."""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_hist")
        await db.update_task("t_hist", {"status": "in_progress"})
        await db.update_task("t_hist", {"status": "completed"})

        first_eid = await db.reopen_task("t_hist")

        # Drive second generation to terminal, then reopen again
        await db.update_task("t_hist", {"status": "in_progress"})
        await db.update_task("t_hist", {"status": "failed"})
        second_eid = await db.reopen_task("t_hist")

        task = await db.get_task("t_hist")
        prev = task["previous_execution_ids"]
        assert first_eid in prev        # first generation traced
        assert second_eid not in prev   # current generation not yet traced
        assert task["execution_id"] == second_eid

        await db.close()