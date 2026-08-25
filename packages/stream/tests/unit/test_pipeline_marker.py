"""MarkerDetector 单测：完整/半 marker、上下文提取、阈值/超时冲刷。"""
import pytest

from orditect.stream.pipeline import MarkerDetector, aiter_from_iterable
from orditect.stream.protocols import SourceChunk


async def _run(chunks, **kw):
    det = MarkerDetector(**kw)
    texts, hits, finished = [], [], False
    async for out in det.process(_chunks_from(chunks)):
        if out.text:
            texts.append(out.text)
        hits.extend(out.hits)
        if out.finish:
            finished = True
    return "".join(texts), hits, finished


async def _chunks_from(texts):
    for t in texts:
        yield SourceChunk(text=t)
    yield SourceChunk(finish=True)


class TestMarkerDetector:
    async def test_full_marker_hit(self):
        text, hits, finished = await _run(["正文。![img]后续。",])
        assert "![img]" not in text
        assert "正文。" in text
        assert "后续。" in text
        assert len(hits) == 1
        assert finished is True

    async def test_half_marker_protection(self):
        # marker split across two chunks
        text, hits, _ = await _run(["前文![im", "g]后文"])
        assert len(hits) == 1
        assert "![img]" not in text
        assert "前文" in text and "后文" in text

    async def test_partial_marker_at_chunk_end_not_flushed(self):
        # chunk tail is marker prefix: must suspend waiting for next block
        text, hits, _ = await _run(["abc![", "img]def"])
        assert len(hits) == 1
        assert "abcdef" in text

    async def test_multiple_markers(self):
        text, hits, _ = await _run(["a![img]b![img]c"])
        assert len(hits) == 2
        assert text == "abc"

    async def test_context_paragraph(self):
        _, hits, _ = await _run(["第一段。\n第二段内容。![img]"], context_strategy="paragraph")
        assert hits[0].context_text == "第二段内容。"

    async def test_context_full(self):
        _, hits, _ = await _run(["第一段。\n第二段内容。![img]"], context_strategy="full")
        assert "第一段。" in hits[0].context_text
        assert "第二段内容。" in hits[0].context_text

    async def test_context_heading(self):
        _, hits, _ = await _run(
            ["## 标题\n标题下内容。![img]"],
            context_strategy="heading",
        )
        assert hits[0].context_text.startswith("## 标题")

    async def test_trailing_marker_at_finish(self):
        # end with tail dangling marker: faithful hit (tail handled by runner)
        text, hits, finished = await _run(["正文![img]"])
        assert len(hits) == 1
        assert "正文" in text
        assert finished is True