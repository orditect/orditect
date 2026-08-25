"""create_stream_response: StreamRunner event stream → SSE StreamingResponse.

Responsibilities:
- SSE frame encoding (reuses sse/frame)
- Response headers: Content-Type / Cache-Control / X-Accel-Buffering / CORS
- Heartbeat (SSEWriter comment frames)
- Disconnect awareness: poll request.is_disconnected() → runner.notify_disconnect()
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from fastapi import Request
from fastapi.responses import StreamingResponse

from orditect.stream.events import EventEnvelope, EventType
from orditect.stream.runner import StreamRunner
from orditect.stream.sse import encode_envelope, encode_heartbeat

_DEFAULT_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",   # 禁 nginx 代理缓冲
}


async def _event_stream(
    runner: StreamRunner,
    request: Request,
    heartbeat_interval: float,
    disconnect_check_interval: float = 0.5,
) -> AsyncIterator[bytes]:
    """Event stream generator: encoding + heartbeat + disconnect awareness."""

    async def watch_disconnect() -> None:
        """Poll client connection status, notify runner on disconnect."""
        while True:
            if await request.is_disconnected():
                await runner.notify_disconnect()
                return
            await asyncio.sleep(disconnect_check_interval)

    watcher = asyncio.create_task(watch_disconnect())
    last_beat = asyncio.get_running_loop().time()

    try:
        async for envelope, event_type in runner.run():
            # grace disconnecting: runner has buffered events, not delivered here
            if runner.should_buffer:
                continue
            yield encode_envelope(envelope, event_type)

            # heartbeat (interspersed comment frames at intervals)
            now = asyncio.get_running_loop().time()
            if now - last_beat >= heartbeat_interval:
                yield encode_heartbeat()
                last_beat = now
    finally:
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass


def create_stream_response(
    runner: StreamRunner,
    request: Request,
    *,
    heartbeat_interval: float | None = None,
    extra_headers: dict[str, str] | None = None,
) -> StreamingResponse:
    """Create SSE StreamingResponse.

    Args:
        runner: StreamRunner instance
        request: FastAPI Request (for disconnect awareness)
        heartbeat_interval: heartbeat interval (None uses config value)
        extra_headers: additional response headers
    """
    hb = heartbeat_interval if heartbeat_interval is not None else 15.0
    headers = {**_DEFAULT_HEADERS, **(extra_headers or {})}

    return StreamingResponse(
        _event_stream(runner, request, hb),
        media_type="text/event-stream",
        headers=headers,
    )