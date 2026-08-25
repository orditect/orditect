"""GraceBuffer: event buffer for disconnection grace period.

After client disconnects:
- Consumer side continues writing events to buffer (not dropping)
- Reconnection: first drain buffer (in seq order), then resume real-time stream
- Capacity is capped (prevent infinite accumulation); when full, oldest events are evicted and gap flag is set
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass

from orditect.stream.events import EventEnvelope, EventType


@dataclass
class BufferedEvent:
    envelope: EventEnvelope
    event_type: EventType


class GraceBuffer:
    """Grace period event buffer."""

    def __init__(self, maxsize: int = 5000):
        self._buf: deque[BufferedEvent] = deque(maxlen=maxsize)
        self._lock = asyncio.Lock()
        self._gap = False  # 是否发生过挤掉（客户端需知悉有序列空洞）

    async def put(self, envelope: EventEnvelope, event_type: EventType) -> None:
        async with self._lock:
            if len(self._buf) == self._buf.maxlen:
                self._gap = True
            self._buf.append(BufferedEvent(envelope, event_type))

    async def drain(self) -> tuple[list[BufferedEvent], bool]:
        """Drain all buffered events, returns (event list, whether there is a gap)."""
        async with self._lock:
            items = list(self._buf)
            self._buf.clear()
            gap = self._gap
            self._gap = False
            return items, gap

    async def size(self) -> int:
        async with self._lock:
            return len(self._buf)