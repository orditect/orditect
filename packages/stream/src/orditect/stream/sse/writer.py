"""SSE writer: frame flushing + heartbeat scheduling + Last-Event-ID parsing (reserved).

Responsibilities:
- Write business event frames and heartbeat frames to the same sink (async callable)
- Heartbeat runs in a separate coroutine, sends comment frames periodically; stops automatically when stream ends
- parse_last_event_id: parse checkpoint anchor (v1 only parses as reserved, does not replay)
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from orditect.stream.events import EventEnvelope, EventType
from orditect.stream.sse.frame import encode_envelope, encode_heartbeat

Sink = Callable[[bytes], Awaitable[None]]


class SSEWriter:
    """SSE writer."""

    def __init__(self, sink: Sink, heartbeat_interval: float = 15.0):
        self._sink = sink
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_task: asyncio.Task | None = None
        self._closed = False

    async def write(self, envelope: EventEnvelope, event_type: EventType) -> None:
        """Write one business event frame."""
        if self._closed:
            raise RuntimeError("SSEWriter already closed")
        await self._sink(encode_envelope(envelope, event_type))

    async def start_heartbeat(self) -> None:
        """Start heartbeat coroutine (idempotent)."""
        if self._heartbeat_task is not None or self._heartbeat_interval <= 0:
            return

        async def _loop() -> None:
            try:
                while True:
                    await asyncio.sleep(self._heartbeat_interval)
                    await self._sink(encode_heartbeat())
            except asyncio.CancelledError:
                return

        self._heartbeat_task = asyncio.create_task(_loop())

    async def close(self) -> None:
        """Stop heartbeat and close (idempotent)."""
        if self._closed:
            return
        self._closed = True
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    async def __aenter__(self) -> "SSEWriter":
        await self.start_heartbeat()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()


def parse_last_event_id(raw: str | None) -> tuple[str, int] | None:
    """Parse Last-Event-ID → (stream_id, seq).

    v1 only parses as reserved (checkpoint anchor for resumption), does not replay.
    Invalid format returns None.
    """
    if not raw:
        return None
    stream_id, sep, seq_str = raw.rpartition(":")
    if not sep or not stream_id:
        return None
    try:
        return stream_id, int(seq_str)
    except ValueError:
        return None