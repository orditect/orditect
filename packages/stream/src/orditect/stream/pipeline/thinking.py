"""Thinking demux + chunk splitter.

Two responsibilities (both are ChunkMiddleware):

1. ChunkSplitter: splits SourceChunk into single-field chunk sequence (references → thinking → text order), making downstream middleware process the purest objects.

2. ThinkingDemux: processes thinking chunks according to thinking_mode:
   - inline: pass through as-is (runner converts to kind=thinking delta)
   - separate: aggregate and cache (expose collect()/pop_collected(), runner takes and puts into stage.end)
   - suppress: discard
"""
from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator

from orditect.stream.config import ThinkingMode
from orditect.stream.protocols import SourceChunk


class ChunkSplitter:
    """Combined chunk → single-field chunk sequence (order: references → thinking → text)."""

    async def process(self, chunks: AsyncIterable[SourceChunk]) -> AsyncIterator[SourceChunk]:
        async for chunk in chunks:
            if chunk.references is not None:
                yield SourceChunk(references=chunk.references)
            if chunk.thinking is not None:
                yield SourceChunk(thinking=chunk.thinking)
            if chunk.text is not None:
                yield SourceChunk(text=chunk.text)
            if chunk.finish:
                yield SourceChunk(finish=True)


class ThinkingDemux:
    """Thinking demux middleware."""

    def __init__(self, mode: ThinkingMode):
        self._mode = mode
        self._collected: list[str] = []

    async def process(self, chunks: AsyncIterable[SourceChunk]) -> AsyncIterator[SourceChunk]:
        async for chunk in chunks:
            if chunk.thinking is not None:
                if self._mode is ThinkingMode.INLINE:
                    yield chunk
                elif self._mode is ThinkingMode.SEPARATE:
                    self._collected.append(chunk.thinking)
                # suppress: discard
            else:
                yield chunk

    def collect(self) -> str:
        """Read the aggregated full thinking text (separate mode)."""
        return "".join(self._collected)

    def pop_collected(self) -> str:
        """Read and clear the aggregated thinking (called by runner at stage.end)."""
        out = self.collect()
        self._collected.clear()
        return out