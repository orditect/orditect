"""Pinning tests for ActionDispatcher bounded dedup (v0.1.6).

Red before: ActionDispatcher._seen was an unbounded set growing for the
process lifetime; a long-running dispatcher leaked memory with the number
of distinct action_ids.
"""

from __future__ import annotations

import asyncio

import pytest

from orditect.flow.actions import ActionDispatcher, ActionType
from orditect.flow.actions.models import ActionCommand, new_action_id

pytestmark = pytest.mark.unit


class _StubQueue:
    """Minimal in-memory queue for dispatcher tests."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[ActionCommand] = asyncio.Queue()
        self._receipts: dict[str, dict] = {}

    async def enqueue(self, command: ActionCommand) -> None:
        await self._queue.put(command)

    async def dequeue(self, *, timeout: float = 1.0) -> ActionCommand | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def get_receipt(self, action_id: str) -> dict | None:
        return self._receipts.get(action_id)

    async def write_receipt(self, action_id: str, receipt: dict) -> None:
        self._receipts[action_id] = receipt


class _RecordingOrchestrator:
    def __init__(self, results: list[bool]) -> None:
        self._results = results
        self.cancelled: list[str] = []

    async def cancel(self, task_id: str, **kwargs) -> bool:
        # Mirrors TaskOrchestrator.cancel's signature shape (task_id
        # positional; extra kwargs tolerated for forward compatibility).
        self.cancelled.append(task_id)
        return self._results.pop(0) if self._results else False


def _pause_command(task_id: str, action_id: str | None = None) -> ActionCommand:
    return ActionCommand(
        action_id=action_id or new_action_id(),
        action_type=ActionType.PAUSE,
        target_task_id=task_id,
    )


class TestBoundedDedup:
    async def test_duplicate_action_skipped_within_window(self):
        queue = _StubQueue()
        orch = _RecordingOrchestrator([True])
        dispatcher = ActionDispatcher(queue, orch, recovery=None, dedup_capacity=10)

        cmd = _pause_command("t1")
        await dispatcher._execute(cmd)
        await dispatcher._execute(cmd)  # duplicate: skipped, no second cancel

        assert orch.cancelled == ["t1"]

    async def test_window_is_bounded(self):
        """Entries beyond dedup_capacity are evicted (bounded memory)."""
        queue = _StubQueue()
        orch = _RecordingOrchestrator([True] * 20)
        dispatcher = ActionDispatcher(queue, orch, recovery=None, dedup_capacity=5)

        ids: list[str] = []
        for i in range(10):
            cmd = _pause_command(f"t{i}")
            ids.append(cmd.action_id)
            await dispatcher._execute(cmd)

        assert len(dispatcher._seen) == 5  # bounded, not 10
        # the oldest 5 action_ids were evicted, the newest 5 retained
        assert all(aid in dispatcher._seen for aid in ids[5:])
        assert all(aid not in dispatcher._seen for aid in ids[:5])

    async def test_evicted_id_reexecutes(self):
        """Documented semantics: a repeated action_id BEYOND the window
        re-executes (dedup guarantee is window-scoped)."""
        queue = _StubQueue()
        orch = _RecordingOrchestrator([True] * 10)
        dispatcher = ActionDispatcher(queue, orch, recovery=None, dedup_capacity=2)

        first = _pause_command("t-old", action_id="act-old")
        await dispatcher._execute(first)
        # push two newer ids through the window, evicting act-old
        await dispatcher._execute(_pause_command("t1", action_id="act-1"))
        await dispatcher._execute(_pause_command("t2", action_id="act-2"))

        # act-old is outside the window: it re-executes
        await dispatcher._execute(first)
        assert orch.cancelled.count("t-old") == 2

    async def test_pause_receipt_still_written(self):
        """Regression: bounded dedup must not disturb receipt writing."""
        queue = _StubQueue()
        orch = _RecordingOrchestrator([True])
        dispatcher = ActionDispatcher(queue, orch, recovery=None, dedup_capacity=10)

        cmd = _pause_command("t1", action_id="act-x")
        await dispatcher._execute(cmd)

        receipt = await queue.get_receipt("act-x")
        assert receipt is not None
        assert receipt["status"] == "executed"