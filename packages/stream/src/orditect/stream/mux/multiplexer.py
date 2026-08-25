"""StreamMux: multi-substream merging + unified seq allocation + backpressure.

- Each substream (stream_id) holds an independent SeqAllocator (monotonically increasing per stream)
- Substreams put (stream_id, event_type, data) into a shared queue; the mux consumer takes them one by one,
  assigns seq, wraps into EventEnvelope and yields
- Backpressure: block (backpressure upstream, default) / fail (BackpressureError)
- When max_id=1, degenerates to passthrough with zero ceremony
- Close semantics:
  - close_stream: normal path, sentinel is awaited for insertion (ensuring it is consumed after all already enqueued events)
  - force_close: abnormal path, sentinel is put_nowait (if full, drop oldest to make room, never introduces new wait points)
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from orditect.stream.config import BackpressurePolicy
from orditect.stream.events import EventEnvelope, EventType
from orditect.stream.exceptions import BackpressureError


class SeqAllocator:
    """Seq allocator for a single stream (monotonically increasing from 1)."""

    def __init__(self) -> None:
        self._seq = 0

    def next(self) -> int:
        self._seq += 1
        return self._seq

    @property
    def current(self) -> int:
        return self._seq


@dataclass
class _QueuedItem:
    stream_id: str
    stage: str | None
    event_type: EventType
    data: dict


class StreamMux:
    """Multi-stream multiplexer."""

    def __init__(self, maxsize: int = 1000, backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK):
        self._queue: asyncio.Queue[_QueuedItem | None] = asyncio.Queue(maxsize=maxsize)
        self._backpressure = backpressure
        self._allocators: dict[str, SeqAllocator] = {}
        self._open_streams: set[str] = set()
        self._closed = False

    # ---- producer side (called by substreams) ----
    def register(self, stream_id: str) -> SeqAllocator:
        """Register a substream, returning its seq allocator."""
        if stream_id not in self._allocators:
            self._allocators[stream_id] = SeqAllocator()
        self._open_streams.add(stream_id)
        return self._allocators[stream_id]

    async def emit(
        self,
        stream_id: str,
        event_type: EventType,
        data: dict,
        stage: str | None = None,
    ) -> None:
        """A substream emits one event (seq not assigned yet; assigned by consumer side)."""
        if self._closed:
            raise RuntimeError("mux closed")
        item = _QueuedItem(stream_id=stream_id, stage=stage, event_type=event_type, data=data)
        if self._backpressure is BackpressurePolicy.BLOCK:
            await self._queue.put(item)
        else:
            if self._queue.full():
                raise BackpressureError(
                    f"mux queue full (maxsize={self._queue.maxsize}), policy=fail"
                )
            self._queue.put_nowait(item)

    async def close_stream(self, stream_id: str) -> None:
        """Mark a substream as finished; when all are finished, inject a sentinel.

        Normal path: when queue is full, await for space — the sentinel must be consumed after all already enqueued events,
        which is the correct backpressure behavior (consumer side is usually pulling continuously).
        """
        self._open_streams.discard(stream_id)
        if not self._open_streams and not self._closed:
            self._closed = True
            await self._queue.put(None)  # 哨兵

    async def force_close(self) -> None:
        """Force close (abnormal path).

        The sentinel must be able to be inserted; never introduce new wait points:
        - put_nowait to insert sentinel
        - if queue is full, drop the oldest one to make room
        - in extreme cases if insertion fails, abandon the sentinel; consumer side will exit via closed+empty fallback.
        """
        if not self._closed:
            self._closed = True
            self._open_streams.clear()
            try:
                self._queue.put_nowait(None)
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()  # 丢一条最老的腾位
                except asyncio.QueueEmpty:
                    pass
                try:
                    self._queue.put_nowait(None)
                except asyncio.QueueFull:
                    pass  # 放弃哨兵，消费侧靠 closed + empty 退出

    # ---- consumer side (called by SSE writer) ----
    async def events(self) -> AsyncIterator[tuple[EventEnvelope, EventType]]:
        """Consume the event stream: uniformly assign seq, yield (envelope, event_type).

        Exit conditions (dual guarantee):
        - Received sentinel (normal/forced close)
        - closed and queue empty (extreme case where force_close abandoned sentinel)
        """
        while True:
            if self._closed and self._queue.empty():
                return
            item = await self._queue.get()
            if item is None:
                return
            allocator = self._allocators.setdefault(item.stream_id, SeqAllocator())
            envelope = EventEnvelope(
                stream_id=item.stream_id,
                stage=item.stage,
                seq=allocator.next(),
                data=item.data,
            )
            yield envelope, item.event_type