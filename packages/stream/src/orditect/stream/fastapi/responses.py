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
    """Event stream generator: encoding + heartbeat + disconnect awareness.

    The heartbeat is produced by an INDEPENDENT coroutine on a fixed
    interval, merged with business frames through a local queue. It must NOT
    be coupled to awaiting the runner's next business event: during a quiet
    period the runner itself is blocked waiting on the mux queue, so a
    heartbeat that only fires while waiting for the runner's next event
    would never fire exactly when it is needed (this was the pre-fix bug).
    Decoupled scheduling guarantees frames are emitted on time even while
    the business stream is completely idle (slow first token, enrich settle
    windows, marker buffering) — proxies and load balancers must never see
    an idle connection mid-stream.
    """

    async def watch_disconnect() -> None:
        """Poll client connection status, notify runner on disconnect."""
        while True:
            if await request.is_disconnected():
                await runner.notify_disconnect()
                return
            await asyncio.sleep(disconnect_check_interval)

    watcher = asyncio.create_task(watch_disconnect())
    event_iter = runner.run().__aiter__()
    out: asyncio.Queue[bytes | None] = asyncio.Queue()
    stop = asyncio.Event()

    async def business_frames() -> None:
        """Forward business frames (real runner path), then a sentinel."""
        try:
            async for envelope, event_type in event_iter:
                if runner.should_buffer:
                    continue  # grace: buffered by runner, not delivered here
                await out.put(encode_envelope(envelope, event_type))
        finally:
            await out.put(None)

    async def heartbeat_frames() -> None:
        """Emit heartbeat comment frames on a fixed interval until stopped."""
        try:
            while not stop.is_set():
                await asyncio.sleep(heartbeat_interval)
                if not stop.is_set():
                    await out.put(encode_heartbeat())
        except asyncio.CancelledError:
            pass

    business_task = asyncio.create_task(business_frames())
    heartbeat_task = asyncio.create_task(heartbeat_frames()) if heartbeat_interval > 0 else None

    try:
        while True:
            frame = await out.get()
            if frame is None:
                break
            yield frame
    finally:
        stop.set()
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        # Business frames may still be blocked putting into the local queue
        # after we stop consuming; drain and cancel it, then close the
        # runner generator so its cleanup runs deterministically.
        business_task.cancel()
        try:
            await business_task
        except asyncio.CancelledError:
            pass
        except StopAsyncIteration:
            pass
        try:
            await event_iter.aclose()
        except RuntimeError:
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
        heartbeat_interval: heartbeat interval in seconds (None = the
            runner's configured StreamConfig.heartbeat_interval)
        extra_headers: additional response headers
    """
    # Fall back to the runner's configured interval (was hardcoded 15.0,
    # silently ignoring StreamConfig.heartbeat_interval).
    hb = (
        heartbeat_interval
        if heartbeat_interval is not None
        else runner._cfg.heartbeat_interval
    )
    headers = {**_DEFAULT_HEADERS, **(extra_headers or {})}

    return StreamingResponse(
        _event_stream(runner, request, hb),
        media_type="text/event-stream",
        headers=headers,
    )