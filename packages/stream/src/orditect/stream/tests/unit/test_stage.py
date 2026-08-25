"""StageRunner 单测（passthrough / replay / llm mock）。"""
import pytest

from orditect.stream.config import DEFAULT_CONFIG, ThinkingMode
from orditect.stream.pipeline import MarkerHit
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.stages import SourceType, StageConfig, StageRunner


class _MockSource:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self, request: SourceRequest):
        for c in self._chunks:
            yield c


async def _run_stage(cfg, scfg=None):
    runner = StageRunner(cfg, scfg or DEFAULT_CONFIG)
    texts, thinkings, refs, hits = [], [], [], []
    outcome = await runner.run(
        on_text=lambda t: texts.append(t) or _noop(),
        on_thinking=lambda t: thinkings.append(t) or _noop(),
        on_references=lambda r: refs.extend(r) or _noop(),
        on_hit=lambda h: hits.append(h) or _noop(),
    )
    return outcome, "".join(texts), thinkings, refs, hits


async def _noop():
    return None


class TestStageRunner:
    async def test_passthrough(self):
        cfg = StageConfig(name="lead", source_type=SourceType.PASSTHROUGH, content="固定内容")
        outcome, text, _, _, _ = await _run_stage(cfg)
        assert "固定内容" in text
        assert outcome.content == text

    async def test_replay(self):
        chunks = [SourceChunk(text="ab"), SourceChunk(text="cd"), SourceChunk(finish=True)]
        cfg = StageConfig(name="main", source_type=SourceType.REPLAY, replay_chunks=chunks)
        outcome, text, _, _, _ = await _run_stage(cfg)
        assert text == "abcd"
        assert outcome.content == "abcd"

    async def test_llm_mock_with_marker(self):
        chunks = [
            SourceChunk(text="正文第一段。"),
            SourceChunk(text="![img]"),
            SourceChunk(text="正文第二段。"),
            SourceChunk(finish=True),
        ]
        cfg = StageConfig(
            name="main", source_type=SourceType.LLM,
            source=_MockSource(chunks),
        )
        outcome, text, _, _, hits = await _run_stage(cfg)
        assert "![img]" not in text
        assert len(hits) == 1
        assert outcome.content == text

    async def test_thinking_separate_collected(self):
        scfg = DEFAULT_CONFIG.merge(thinking_mode=ThinkingMode.SEPARATE)
        chunks = [
            SourceChunk(thinking="想一下"),
            SourceChunk(text="正文"),
            SourceChunk(finish=True),
        ]
        cfg = StageConfig(
            name="main", source_type=SourceType.LLM, source=_MockSource(chunks)
        )
        outcome, text, thinkings, _, _ = await _run_stage(cfg, scfg)
        assert thinkings == []           # separate 不透传
        assert outcome.thinking == "想一下"

    async def test_thinking_inline_passthrough(self):
        scfg = DEFAULT_CONFIG.merge(thinking_mode=ThinkingMode.INLINE)
        chunks = [SourceChunk(thinking="想一下"), SourceChunk(text="正文"), SourceChunk(finish=True)]
        cfg = StageConfig(name="main", source_type=SourceType.LLM, source=_MockSource(chunks))
        _, text, thinkings, _, _ = await _run_stage(cfg, scfg)
        assert thinkings == ["想一下"]