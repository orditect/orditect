"""StreamExecutor: stage sequence executor for a single substream.

Responsibilities:
- Execute the stage list of this substream in sequence (serial stages in two levels; parallel stages use asyncio.gather within groups)
- Convert incremental signals from StageRunner into events and emit them via mux.emit
- Collect stage.end and aggregation results for use by finalizer/manifest
- Pass governor and cancel_token to StageRunner

Not responsible for:
- mux consumption and SSE encoding (handled by StreamRunner/SSE layer)
- Enrich task dispatching (handled by EnrichManager; this class only triggers on_hit callback)
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from orditect.stream.config import StreamConfig
from orditect.stream.core import CancellationToken
from orditect.stream.events import (
    EventType,
    DeltaKind,
    make_delta,
    make_stage_end,
)
from orditect.stream.mux import StreamMux
from orditect.stream.pipeline import MarkerHit
from orditect.stream.protocols.governor import ResourceGovernorProtocol
from orditect.stream.stages import StageConfig, StageOutcome, StageRunner
from orditect.stream.stream_result import StreamResult


class StreamExecutor:
    """Single-stream executor."""

    def __init__(
        self,
        stream_id: str,
        stages: list[StageConfig],
        config: StreamConfig,
        mux: StreamMux,
        on_hit: "OnHitCallback | None" = None,
        governor: ResourceGovernorProtocol | None = None,
        cancel_token: CancellationToken | None = None,
    ):
        self._stream_id = stream_id
        self._stages = stages
        self._cfg = config
        self._mux = mux
        self._on_hit = on_hit or _noop_hit
        self._governor = governor
        self._cancel_token = cancel_token or CancellationToken()

    async def run(self) -> StreamResult:
        """Execute all stages and return aggregated result."""
        result = StreamResult(stream_id=self._stream_id)

        serial_stages = [s for s in self._stages if s.mode == "serial"]
        parallel_stages = [s for s in self._stages if s.mode == "parallel"]

        # serial group execute sequentially
        for stage_cfg in serial_stages:
            outcome = await self._run_stage(stage_cfg, result)
            result.stages[stage_cfg.name] = outcome

        # parallel group execute concurrently
        if parallel_stages:
            outcomes = await asyncio.gather(
                *(self._run_stage(s, result) for s in parallel_stages)
            )
            for stage_cfg, outcome in zip(parallel_stages, outcomes):
                result.stages[stage_cfg.name] = outcome

        return result

    async def _run_stage(self, stage_cfg: StageConfig, result: StreamResult) -> StageOutcome:
        """Execute a single stage: signals → event emission."""
        runner = StageRunner(
            stage_cfg,
            self._cfg,
            governor=self._governor,
            cancel_token=self._cancel_token,
        )
        sid = self._stream_id
        stage_name = stage_cfg.name

        async def on_text(text: str) -> None:
            await self._mux.emit(
                sid, EventType.STREAM_DELTA,
                make_delta(DeltaKind.CONTENT, text=text),
                stage=stage_name,
            )

        async def on_thinking(text: str) -> None:
            await self._mux.emit(
                sid, EventType.STREAM_DELTA,
                make_delta(DeltaKind.THINKING, text=text),
                stage=stage_name,
            )

        async def on_references(refs: list[dict[str, Any]]) -> None:
            await self._mux.emit(
                sid, EventType.STREAM_DELTA,
                make_delta(DeltaKind.REFERENCES, references=refs),
                stage=stage_name,
            )

        async def on_hit(hit: MarkerHit) -> None:
            result.hits.append(hit)
            await self._on_hit(self._stream_id, stage_name, hit)

        outcome = await runner.run(
            on_text=on_text,
            on_thinking=on_thinking,
            on_references=on_references,
            on_hit=on_hit,
        )

        # stage.end event
        await self._mux.emit(
            sid, EventType.STAGE_END,
            make_stage_end(
                name=stage_name,
                content=outcome.content,
                thinking=outcome.thinking or None,
                usage=outcome.usage,
            ),
            stage=stage_name,
        )
        return outcome


# ---- on_hit callback signature (StreamRunner injects EnrichManager, B4 connects implementation) ----
from collections.abc import Awaitable, Callable

OnHitCallback = Callable[[str, str, MarkerHit], Awaitable[None]]


async def _noop_hit(stream_id: str, stage: str, hit: MarkerHit) -> None:
    return None