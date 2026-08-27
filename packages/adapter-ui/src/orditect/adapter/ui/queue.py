"""In-memory ActionQueue reference implementation (single-process, testing).

Production deployments should use a hot-path Redis-backed queue (the
dispatcher polls a Redis list). This in-memory stub exists for the reference
UI adapter and for tests.
"""

from __future__ import annotations

import asyncio
from typing import Any

from orditect.flow.actions.models import ActionCommand


class MemoryActionQueue:
    """In-memory action queue (single-process reference)."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[ActionCommand] = asyncio.Queue()
        self._receipts: dict[str, dict[str, Any]] = {}

    async def enqueue(self, command: ActionCommand) -> None:
        await self._queue.put(command)

    async def dequeue(self, *, timeout: float = 1.0) -> ActionCommand | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def get_receipt(self, action_id: str) -> dict[str, Any] | None:
        return self._receipts.get(action_id)

    async def write_receipt(self, action_id: str, receipt: dict[str, Any]) -> None:
        """Write the execution receipt (called by the dispatcher)."""
        self._receipts[action_id] = receipt