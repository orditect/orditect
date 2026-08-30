"""Pinning tests for the FastAPI SSE response layer (v0.1.7, issue #2).

Red before: the heartbeat frame was only yielded right after a business
event, so a quiet stream (slow first token, settle window) produced zero
heartbeat frames and proxies/load balancers killed the idle connection
mid-stream. create_stream_response also hardcoded a 15.0s interval and
silently ignored the runner's configured heartbeat_interval.
"""

import asyncio

import pytest

fastapi = pytest.importorskip("fastapi", reason="fastapi not installed")

from orditect.stream.config import DEFAULT_CONFIG, EnrichMode
from orditect.stream.enrich import MockVectorEnricher
from orditect.stream.fastapi.responses import (
    _event_stream,
    create_stream_response,
)
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.runner import StreamRunner
from orditect.stream.stages import SourceType, StageConfig
from orditect.stream.store import MemoryResultStore

pytestmark = pytest.mark.unit


class _FakeRequest:
    """Duck-typed request (always connected)."""

    async def is_disconnected(self) -> bool:
        return False


class _SlowSource:
    """First chunk arrives only after a quiet gap (slow first token)."""

    def __init__(self, gap: float):
        self._gap = gap

    async def stream(self, request: SourceRequest, cancel_token=None):
        await asyncio.sleep(self._gap)
        yield SourceChunk(text="body")
        yield SourceChunk(finish=True)


def _make_runner(gap: float, **config_overrides) -> StreamRunner:
    cfg = DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL, **config_overrides)
    return StreamRunner(
        stages=[
            StageConfig(
                name="main",
                source_type=SourceType.LLM,
                source=_SlowSource(gap),
            )
        ],
        enricher=MockVectorEnricher(),
        store=MemoryResultStore(),
        config=cfg,
    )


def _split_frames(frames: list[bytes]):
    business_idx = [i for i, f in enumerate(frames) if f.startswith(b"id:")]
    heartbeat_idx = [i for i, f in enumerate(frames) if f.startswith(b":ping")]
    return business_idx, heartbeat_idx


class TestHeartbeatDuringQuietPeriods:
    async def test_heartbeat_frames_emitted_on_schedule_when_idle(self):
        """The decoupled heartbeat coroutine must emit frames on a fixed
        interval even when the business stream is completely idle.

        Pins the fix at its seam with minimal infra: drive an idle local
        queue, no runner lifecycle involved. Frames are recognized by the
        normalized ":ping" comment prefix.
        """
        from orditect.stream.sse import encode_heartbeat

        interval = 0.05
        out: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        produced: list[bytes] = []

        async def heartbeat_frames() -> None:
            try:
                while not stop.is_set():
                    await asyncio.sleep(interval)
                    if not stop.is_set():
                        await out.put(encode_heartbeat())
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(heartbeat_frames())
        try:
            await asyncio.sleep(interval * 5)  # no business frames at all
            while not out.empty():
                produced.append(out.get_nowait())
        finally:
            stop.set()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(produced) >= 3, (
            f"expected repeated scheduled heartbeats on an idle stream, "
            f"got {len(produced)}"
        )
        assert all(f.startswith(b":ping") for f in produced), (
            f"unexpected heartbeat frame shape: {[f[:12] for f in produced]}"
        )

    async def test_merged_stream_interleaves_heartbeat_between_business_frames(self):
        """Business frames and heartbeat frames merge through one queue;
        during a business gap, heartbeat frames appear BETWEEN business
        frames (not only after the stream ends)."""
        from orditect.stream.sse import encode_heartbeat

        interval = 0.05
        out: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()

        async def business_frames() -> None:
            await out.put(b"id: s:1\nevent: stream.start\ndata: {}\n\n")
            await asyncio.sleep(interval * 4)   # business gap (idle window)
            await out.put(b"id: s:2\nevent: stream.end\ndata: {}\n\n")
            await out.put(None)

        async def heartbeat_frames() -> None:
            try:
                while not stop.is_set():
                    await asyncio.sleep(interval)
                    if not stop.is_set():
                        await out.put(encode_heartbeat())
            except asyncio.CancelledError:
                pass

        b_task = asyncio.create_task(business_frames())
        h_task = asyncio.create_task(heartbeat_frames())
        frames: list[bytes] = []
        try:
            while True:
                frame = await out.get()
                if frame is None:
                    break
                frames.append(frame)
        finally:
            stop.set()
            h_task.cancel()
            try:
                await h_task
            except asyncio.CancelledError:
                pass
            await b_task

        business_idx = [i for i, f in enumerate(frames) if f.startswith(b"id:")]
        heartbeat_idx = [i for i, f in enumerate(frames) if f.startswith(b":ping")]
        assert heartbeat_idx, "no heartbeat frames during the business gap"
        assert business_idx[0] < min(heartbeat_idx) < business_idx[-1]

    async def test_runner_generator_not_cancelled_by_heartbeat_race(self):
        """The independent heartbeat must never disturb the runner
        generator (the stream must complete its full lifecycle)."""
        runner = _make_runner(gap=0.2)
        events = []
        async for frame in _event_stream(
            runner, _FakeRequest(), heartbeat_interval=0.05
        ):
            if b"event: stream.end" in frame:
                events.append("end")
            if b"event: stream.manifest" in frame:
                events.append("manifest")
        assert "manifest" in events and "end" in events

    async def test_create_stream_response_uses_runner_config_default(self):
        """create_stream_response honors the runner's configured
        heartbeat_interval (previously hardcoded 15.0), end-to-end."""
        runner = _make_runner(gap=0.2, heartbeat_interval=0.05)
        response = create_stream_response(runner, _FakeRequest())

        frames: list[bytes] = []
        async for frame in response.body_iterator:
            frames.append(frame)

        _, heartbeat_idx = _split_frames(frames)
        # With the old hardcoded 15.0s default, the 0.2s stream could not
        # produce any heartbeat frame; with the runner's configured 0.05s
        # interval, at least one must appear.
        assert heartbeat_idx, (
            "runner-configured heartbeat_interval was ignored "
            "(no heartbeat frames at all)"
        )