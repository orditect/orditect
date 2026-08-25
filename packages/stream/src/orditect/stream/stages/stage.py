"""Stage configuration and single stage executor.

StageRunner responsibilities:
- Produce SourceChunk stream by source_type (llm / passthrough / replay)
- Pass through pipeline: ChunkSplitter → ThinkingDemux → [thinking interception] → MarkerDetector
- MarkedChunk → signals (text/hit/finish)
- Text → on_text callback; hit → on_hit callback (P0: char_offset backfill)
- Aggregate stage result (content/thinking) for stage.end and manifest usage
- Resource governance: acquire/release (LLM special: hold until LLM truly ends)
"""
from __future__ import annotations
import asyncio
import logging

logger = logging.getLogger(__name__)
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from orditect.stream.config import StreamConfig
from orditect.stream.core import CancellationToken
from orditect.stream.pipeline import (
    ChunkSplitter,
    MarkerDetector,
    MarkerHit,
    ThinkingDemux,
)
from orditect.stream.protocols import (
    LLMSourceProtocol,
    SourceChunk,
    SourceRequest,
)
from orditect.stream.protocols.governor import ResourceGovernorProtocol

# framework built-in: stream total flow LLM resource name (special handling: semaphore held until LLM actually ends)
DEFAULT_STREAM_LLM_RESOURCE = "default_stream_llm"


class SourceType(str, Enum):
    """Stage source type."""

    LLM = "llm"                # 真实 LLM 源
    PASSTHROUGH = "passthrough"  # 直通固定内容
    REPLAY = "replay"          # 重放历史结果（演示/测试）


@dataclass(frozen=True)
class StageConfig:
    """Stage configuration (name is arbitrary, framework has no semantic meaning)."""

    name: str
    source_type: SourceType
    source: LLMSourceProtocol | None = None     # llm 类型必填
    content: str | None = None                  # passthrough 必填
    replay_chunks: list[SourceChunk] | None = None  # replay 必填
    request: SourceRequest = field(default_factory=SourceRequest)
    mode: Literal["serial", "parallel"] = "serial"
    resource: str | None = None  # 资源类型（如 "default_stream_llm", "vector_search"）


@dataclass
class StageOutcome:
    """Stage execution aggregate result (for stage.end / manifest)."""

    content: str = ""
    thinking: str = ""
    usage: dict[str, Any] | None = None
    hits: list[MarkerHit] = field(default_factory=list)


# ---- callback signatures injected by runner ----
OnTextDelta = Callable[[str], Awaitable[None]]
OnThinkingDelta = Callable[[str], Awaitable[None]]
OnReferencesDelta = Callable[[list[dict[str, Any]]], Awaitable[None]]
OnMarkerHit = Callable[[MarkerHit], Awaitable[None]]


class StageRunner:
    """Single stage executor."""

    def __init__(
        self,
        config: StageConfig,
        stream_config: StreamConfig,
        governor: ResourceGovernorProtocol | None = None,
        cancel_token: CancellationToken | None = None,
    ):
        self._cfg = config
        self._scfg = stream_config
        self._governor = governor
        self._cancel_token = cancel_token or CancellationToken()
        self._demux = ThinkingDemux(stream_config.thinking_mode)
        self._detector = MarkerDetector(
            marker=stream_config.marker,
            flush_threshold=stream_config.marker_flush_threshold,
            flush_timeout=stream_config.marker_flush_timeout,
            context_strategy=stream_config.enrich_context_strategy,
        )

    async def _source_chunks(self, cancel_token: CancellationToken) -> AsyncIterator[SourceChunk]:
        """Produce a stream of SourceChunks by source_type."""
        if self._cfg.source_type is SourceType.PASSTHROUGH:
            yield SourceChunk(text=self._cfg.content or "")
            yield SourceChunk(finish=True)
            return
        if self._cfg.source_type is SourceType.REPLAY:
            for c in self._cfg.replay_chunks or []:
                yield c
            yield SourceChunk(finish=True)
            return
        # LLM
        assert self._cfg.source is not None
        async for chunk in self._cfg.source.stream(self._cfg.request, cancel_token=cancel_token):
            yield chunk

    async def run(
        self,
        on_text: OnTextDelta,
        on_thinking: OnThinkingDelta,
        on_references: OnReferencesDelta,
        on_hit: OnMarkerHit,
    ) -> StageOutcome:
        """Execute stage, emit incremental signals via callbacks, return aggregate result.

                Resource governance:
                - default_stream_llm: hold semaphore until LLM truly ends (in finally, including continuing consumption after cancel)
                - Other resources: release when stage completes (in finally)

                In finally, close pipeline first then release resource - when drain timeout forces termination, close async generator chain to trigger underlying httpx.stream async with exit, so HTTP connection no longer hangs until GC.
                """
        outcome = StageOutcome()
        resource = self._cfg.resource
        token: str | None = None
        pipeline = None  # #9：声明在外，finally 可达

        # acquire resource token
        if resource and self._governor:
            token = await self._governor.acquire(
                resource,
                timeout=self._scfg.governor_timeout,
            )

        try:
            pipeline = self._detector.process(
                self._intercept_thinking_and_refs(
                    self._demux.process(
                        ChunkSplitter().process(self._source_chunks(self._cancel_token))
                    ),
                    on_thinking,
                    on_references,
                )
            )
            # 1b: fallback start for consuming after cancel (None=not cancelled)
            drain_deadline: float | None = None
            loop = asyncio.get_running_loop()

            async for marked in pipeline:
                # check cancel: stop output but keep consuming (keep LLM connection, semaphore not released)
                if self._cancel_token.is_cancelled():
                    # 1b: set fallback limit when first entering consumption mode
                    if drain_deadline is None:
                        drain_deadline = loop.time() + self._scfg.post_cancel_drain_timeout
                    # 1b: exceeded fallback limit, abandon consumption and force cleanup (prevent LLM hang occupying semaphore permanently)
                    if loop.time() > drain_deadline:
                        logger.warning(
                            f"Post-cancel drain timeout "
                            f"({self._scfg.post_cancel_drain_timeout}s), "
                            f"abandoning LLM stream: stage={self._cfg.name}"
                        )
                        break
                    # not yield, but continue consuming (keep LLM connection)
                    continue

                if marked.text:
                    outcome.content += marked.text
                    await on_text(marked.text)
                for hit in marked.hits:
                    hit.char_offset = len(outcome.content)
                    outcome.hits.append(hit)
                    await on_hit(hit)
                if marked.finish:
                    break

            # separate mode: take aggregated thinking
            outcome.thinking = self._demux.pop_collected()
            return outcome

        finally:
            # v0.3.2 (#9): close pipeline first (trigger source-side async with exit,
            # release httpx connection), then return resource token
            if pipeline is not None:
                try:
                    await pipeline.aclose()
                except Exception:
                    logger.debug(
                        f"pipeline aclose failed (ignored): stage={self._cfg.name}"
                    )
            if token and self._governor:
                await self._governor.release(resource, token)

    async def _intercept_thinking_and_refs(
        self,
        chunks: AsyncIterator[SourceChunk],
        on_thinking: OnThinkingDelta,
        on_references: OnReferencesDelta,
    ) -> AsyncIterator[SourceChunk]:
        """Intercept thinking / references chunks after demux (do not enter detector).

        Changes:
        - T1: intercept references callback here - fix "references chunk was swallowed entirely by MarkerDetector (text=None and not finish → continue)", DeltaKind.REFERENCES events were never emitted before.
        - T2: stop emitting thinking/references after cancel as well - fix semantic gap where "cancel only stops content, thinking continues streaming".
        """
        async for chunk in chunks:
            if chunk.thinking is not None:
                if not self._cancel_token.is_cancelled():
                    await on_thinking(chunk.thinking)
                continue  # thinking 不进 marker detector
            if chunk.references is not None:
                if not self._cancel_token.is_cancelled():
                    await on_references(chunk.references)
                continue  # references 不进 marker detector
            yield chunk