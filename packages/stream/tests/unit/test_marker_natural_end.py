"""MarkerDetector natural-end handling (P0: tail drop regression lock).

The LLMSourceProtocol explicitly allows a source to end WITHOUT a finish
chunk (natural end). Before the fix, MarkerDetector only flushed its
buffered tail on finish=True, so the tail of every real LLM stream was
silently discarded (fast endpoints never trigger the flush timeout, and
final paragraphs usually carry no flush trigger).
"""
from __future__ import annotations

import pytest

from orditect.stream.pipeline import (
    ChunkSplitter,
    MarkerDetector,
    aiter_from_iterable,
)
from orditect.stream.protocols import SourceChunk


async def _collect(chunks, **detector_kwargs):
    detector = MarkerDetector(**detector_kwargs)
    out = [
        marked
        async for marked in detector.process(
            ChunkSplitter().process(aiter_from_iterable(chunks))
        )
    ]
    return out


def _text(out) -> str:
    return "".join(m.text or "" for m in out)


def _hits(out) -> list:
    return [h for m in out for h in m.hits]


class TestNaturalEnd:
    @pytest.mark.asyncio
    async def test_tail_flushed_without_finish_chunk(self):
        """A stream ending without finish=True still yields its tail."""
        chunks = [
            SourceChunk(text="First paragraph with enough text to pass.\n"),
            SourceChunk(text="Final short tail"),
            # no finish chunk: natural end, the LLM path shape
        ]
        out = await _collect(chunks)
        assert "Final short tail" in _text(out)
        assert _text(out).endswith("Final short tail")

    @pytest.mark.asyncio
    async def test_implicit_finish_emitted(self):
        """Natural end yields a terminal finish marker for downstream."""
        out = await _collect([SourceChunk(text="hello world")])
        assert out[-1].finish is True

    @pytest.mark.asyncio
    async def test_trailing_marker_still_hits_on_natural_end(self):
        """A marker at the very end of a naturally-ended stream hits."""
        out = await _collect([
            SourceChunk(text="intro text "),
            SourceChunk(text="![img]"),
        ])
        hits = _hits(out)
        assert len(hits) == 1
        assert "intro text" in hits[0].context_text
        # the marker itself is consumed, not leaked into the text
        assert "![img]" not in _text(out)

    @pytest.mark.asyncio
    async def test_partial_marker_prefix_at_end_kept_verbatim(self):
        """An incomplete marker prefix at natural end is output as text."""
        out = await _collect([SourceChunk(text="body ends with ![im")])
        assert _text(out) == "body ends with ![im"
        assert _hits(out) == []

    @pytest.mark.asyncio
    async def test_explicit_finish_behavior_unchanged(self):
        """The explicit-finish path keeps its exact previous semantics."""
        chunks = [
            SourceChunk(text="some text "),
            SourceChunk(text="![img]"),
            SourceChunk(text="after"),
            SourceChunk(finish=True),
        ]
        out = await _collect(chunks)
        assert _text(out) == "some text after"
        assert len(_hits(out)) == 1
        assert out[-1].finish is True

    @pytest.mark.asyncio
    async def test_empty_stream_natural_end(self):
        """No chunks at all: exactly one finish marker, no text."""
        out = await _collect([])
        assert len(out) == 1
        assert out[0].finish is True

    @pytest.mark.asyncio
    async def test_multiple_tail_markers_each_hit(self):
        """Several markers in the residual buffer hit individually."""
        out = await _collect([
            SourceChunk(text="a ![img] b ![img] c"),
        ])
        assert len(_hits(out)) == 2
        assert _text(out) == "a  b  c"