"""In-memory ActionQueue reference implementation (single-process, testing).

Production deployments should use a hot-path Redis-backed queue (the
dispatcher polls a Redis list). This in-memory stub exists for the reference
UI adapter and for tests.

Receipt retention is bounded (v0.1.6): receipts are kept only for the most
recent `max_receipts` action_ids; older receipts are evicted and
`get_receipt` then returns None. Within the window, receipt lookup is exact.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Any

from orditect.flow.actions.models import ActionCommand


class MemoryActionQueue:
    """In-memory action queue (single-process reference)."""

    def __init__(self, *, max_receipts: int = 10000) -> None:
        self._queue: asyncio.Queue[ActionCommand] = asyncio.Queue()
        self._max_receipts = max_receipts
        self._receipts: OrderedDict[str, dict[str, Any]] = OrderedDict()

    async def enqueue(self, command: ActionCommand) -> None:
        await self._queue.put(command)

    async def dequeue(self, *, timeout: float = 1.0) -> ActionCommand | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def get_receipt(self, action_id: str) -> dict[str, Any] | None:
        receipt = self._receipts.get(action_id)
        if receipt is not None:
            self._receipts.move_to_end(action_id)
        return receipt

    async def write_receipt(self, action_id: str, receipt: dict[str, Any]) -> None:
        """Write the execution receipt (called by the dispatcher).

        Evicts the oldest receipt once the retention window is full.
        """
        if action_id in self._receipts:
            self._receipts.move_to_end(action_id)
        self._receipts[action_id] = receipt
        if len(self._receipts) > self._max_receipts:
            self._receipts.popitem(last=False)