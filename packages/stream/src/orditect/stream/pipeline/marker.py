"""Placeholder marker detection (built-in half-marker protection + context extraction).

Input: single-field SourceChunk (via ChunkSplitter)
Output: MarkedChunk (text + list of MarkerHit signals)

Behavior:
- Text buffered, scan for complete marker:
  - Hit: safe-flush text before marker; produce MarkerHit (char_offset filled later by StageRunner)
  - Buffer tail is incomplete marker prefix: hold back (half-marker protection)
  - Threshold reached with newline/heading, or timeout: flush "safe part" (keep incomplete prefix)
- finish: flush all remaining text; trailing markers still hit (framework faithfully outputs LLM content, never discards)
- Natural end (upstream iterable exhausts without a finish chunk): the
  LLMSourceProtocol explicitly allows it; the residual buffer is flushed
  and an implicit finish marker is emitted, so the tail is never dropped.

Context extraction strategy (config.enrich_context_strategy):
- paragraph: last paragraph before hit (split by \n)
- heading:   from nearest markdown heading line
- full:      entire segment
"""
from __future__ import annotations

import re
import time
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field

from orditect.stream.protocols import SourceChunk

_HEADING_RE = re.compile(r"^#{1,6}\s", re.MULTILINE)
# flush trigger: newline (but not followed by marker) or heading line (P2 UX optimization: avoid cutting sentences in the middle)
_FLUSH_TRIGGER_RE = re.compile(r"[\r\n]+(?!\s*\!\[)|^#{1,6} ", re.MULTILINE)


@dataclass
class MarkerHit:
    """Marker hit signal.

    placeholder_id assigned by EnrichManager after allocation;
    char_offset filled by StageRunner during aggregation (manifest position anchor, P0).
    """

    context_text: str
    placeholder_id: str | None = None
    char_offset: int = 0


@dataclass
class MarkedChunk:
    """Output block from marker middleware: text + list of hit signals."""

    text: str | None = None
    hits: list[MarkerHit] = field(default_factory=list)
    finish: bool = False


class MarkerDetector:
    """Placeholder marker detection middleware."""

    def __init__(
        self,
        marker: str = "![img]",
        flush_threshold: int = 50,
        flush_timeout: float = 0.1,
        context_strategy: str = "paragraph",
    ):
        self._marker = marker
        self._flush_threshold = flush_threshold
        self._flush_timeout = flush_timeout
        self._context_strategy = context_strategy

    # ---- half marker protection ----
    def _rtrim_partial_marker(self, s: str) -> tuple[str, str]:
        """If tail is an incomplete marker prefix, split into (safe_part, pending_prefix)."""
        for i in range(len(self._marker) - 1, 0, -1):
            if s.endswith(self._marker[:i]):
                return s[:-i], s[-i:]
        return s, ""

    # """Extract context according to strategy."""
    def _extract_context(self, preceding: str) -> str:
        text = preceding.strip()
        if not text:
            return ""
        if self._context_strategy == "full":
            return text
        if self._context_strategy == "heading":
            matches = list(_HEADING_RE.finditer(text))
            if matches:
                return text[matches[-1].start():].strip()
            return text
        # paragraph: last paragraph
        paragraphs = [p for p in re.split(r"\n+", text) if p.strip()]
        return paragraphs[-1].strip() if paragraphs else ""

    async def _drain_buffer(self, buffer: str) -> AsyncIterator[MarkedChunk]:
        """Flush the residual buffer at stream end.

        Complete markers still in the buffer hit normally (the framework
        stays faithful to the LLM output and never discards); the text
        preceding each marker is flushed BEFORE the hit is yielded so the
        downstream char_offset backfill stays exact.
        """
        while True:
            pos = buffer.find(self._marker)
            if pos == -1:
                break
            preceding = buffer[:pos]
            if preceding:
                yield MarkedChunk(text=preceding)
            yield MarkedChunk(
                hits=[MarkerHit(context_text=self._extract_context(preceding))]
            )
            buffer = buffer[pos + len(self._marker):]
        if buffer:
            yield MarkedChunk(text=buffer)

    async def process(self, chunks: AsyncIterable[SourceChunk]) -> AsyncIterator[MarkedChunk]:
        buffer = ""
        last_flush = time.monotonic()

        async for chunk in chunks:
            # non-text blocks pass through ignored
            if chunk.text is None and not chunk.finish:
                continue

            if chunk.finish:
                # explicit end: flush the residual buffer, tail markers
                # still hit, then terminate the marked stream
                async for out in self._drain_buffer(buffer):
                    yield out
                yield MarkedChunk(finish=True)
                return

            buffer += chunk.text or ""
            now = time.monotonic()

            # process all complete markers in buffer
            while True:
                pos = buffer.find(self._marker)
                if pos == -1:
                    break
                preceding = buffer[:pos]
                if preceding:
                    yield MarkedChunk(text=preceding)
                hit = MarkerHit(context_text=self._extract_context(preceding))
                buffer = buffer[pos + len(self._marker):]
                last_flush = now
                yield MarkedChunk(hits=[hit])

            # threshold reached with a trigger point, or timeout: flush the
            # safe part (P2: the trigger regex avoids cutting sentences)
            if ((len(buffer) >= self._flush_threshold and _FLUSH_TRIGGER_RE.search(buffer))
                    or (now - last_flush > self._flush_timeout)):
                safe, pending = self._rtrim_partial_marker(buffer)
                if safe:
                    yield MarkedChunk(text=safe)
                    buffer = pending
                    last_flush = now

        # Natural end (no finish chunk): the LLMSourceProtocol explicitly
        # allows it. Treat it as an implicit finish so the buffered tail
        # is never dropped silently.
        async for out in self._drain_buffer(buffer):
            yield out
        yield MarkedChunk(finish=True)