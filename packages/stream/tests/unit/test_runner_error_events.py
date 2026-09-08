"""Runner failure-path wiring (P1: stream.error event emission).

Before the fix, a failed executor only landed in manifest.errors; no
stream.error event was emitted, so consumers that never read the manifest
archived partial bodies as successes. ErrorCode.UPSTREAM_INTERRUPTED was
defined in the frozen enum but never referenced anywhere.
"""
from __future__ import annotations

import pytest

from orditect.stream import (
    DEFAULT_CONFIG,
    EnrichMode,
    EventType,
    MemoryResultStore,
    MockVectorEnricher,
    SourceChunk,
    SourceRequest,
    StageConfig,
    SourceType,
    StreamRunner,
)
from orditect.stream.exceptions import StreamTruncatedError


class _FailingSource:
    """LLM source that raises mid-stream (simulated upstream truncation)."""

    def __init__(self, exc: Exception):
        self._exc = exc

    async def stream(self, request: SourceRequest, cancel_token=None):
        yield SourceChunk(text="partial body ")
        raise self._exc


class _OkSource:
    async def stream(self, request: SourceRequest, cancel_token=None):
        yield SourceChunk(text="full body")
        yield SourceChunk(finish=True)


def _make_runner(source) -> StreamRunner:
    return StreamRunner(
        stages=[StageConfig(
            name="main", source_type=SourceType.LLM, source=source)],
        enricher=MockVectorEnricher(),
        store=MemoryResultStore(),
        config=DEFAULT_CONFIG.merge(enrich_mode=EnrichMode.LOCAL),
    )


class TestExecutorFailureEvents:
    @pytest.mark.asyncio
    async def test_truncation_emits_upstream_interrupted(self):
        """A StreamTruncatedError surfaces as stream.error UPSTREAM_INTERRUPTED."""
        runner = _make_runner(_FailingSource(
            StreamTruncatedError("closed without [DONE]", chunks=3)))
        events = [(env, et) async for env, et in runner.run()]
        types = [et for _, et in events]

        assert EventType.STREAM_ERROR in types
        err = next(env for env, et in events if et is EventType.STREAM_ERROR)
        assert err.data["code"] == "UPSTREAM_INTERRUPTED"
        # optional fields are omitted at their default value
        # (docs/protocol.md: read with .get()); False is the default.
        assert err.data.get("retryable", False) is False

        # protocol discipline: stream.end stays the only terminal signal
        assert types.index(EventType.STREAM_ERROR) < types.index(
            EventType.STREAM_MANIFEST)
        assert types[-1] is EventType.STREAM_END

    @pytest.mark.asyncio
    async def test_generic_failure_emits_internal_error(self):
        """A generic executor failure surfaces as stream.error INTERNAL."""
        runner = _make_runner(_FailingSource(RuntimeError("boom")))
        events = [(env, et) async for env, et in runner.run()]
        err = next(env for env, et in events
                   if et is EventType.STREAM_ERROR)
        assert err.data["code"] == "INTERNAL"

    @pytest.mark.asyncio
    async def test_manifest_records_failure(self):
        """The manifest errors summary carries the failure as well."""
        runner = _make_runner(_FailingSource(RuntimeError("boom")))
        manifest = None
        async for env, et in runner.run():
            if et is EventType.STREAM_MANIFEST:
                manifest = env.data
        assert manifest is not None
        assert manifest["errors"]
        assert manifest["errors"][0]["code"] == "INTERNAL"

    @pytest.mark.asyncio
    async def test_success_path_has_no_error_event(self):
        """A healthy stream emits no stream.error (regression guard)."""
        runner = _make_runner(_OkSource())
        types = [et async for _, et in runner.run()]
        assert EventType.STREAM_ERROR not in types
        assert types[-1] is EventType.STREAM_END