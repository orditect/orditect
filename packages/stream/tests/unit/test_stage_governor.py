"""StageRunner governor 集成测试。"""
import pytest

from orditect.stream.config import DEFAULT_CONFIG
from orditect.stream.core import CancellationToken
from orditect.stream.protocols import SourceChunk, SourceRequest
from orditect.stream.protocols.governor import ResourceGovernorProtocol
from orditect.stream.stages import (
    DEFAULT_STREAM_LLM_RESOURCE,
    SourceType,
    StageConfig,
    StageRunner,
)


class MockGovernor:
    """Mock governor。"""

    def __init__(self):
        self.tokens: dict[str, str] = {}
        self.released: list[tuple[str, str]] = []

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        token = f"token_{resource}_{len(self.tokens)}"
        self.tokens[token] = resource
        return token

    async def release(self, resource: str, token: str) -> None:
        if token in self.tokens:
            del self.tokens[token]
            self.released.append((resource, token))

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def get_usage(self, resource: str) -> int:
        return len([t for t, r in self.tokens.items() if r == resource])


class _MockSource:
    def __init__(self, chunks, delay: float = 0.0):
        self._chunks = chunks
        self._delay = delay

    async def stream(self, request: SourceRequest, cancel_token: CancellationToken | None = None):
        for c in self._chunks:
            if self._delay > 0:
                import asyncio
                await asyncio.sleep(self._delay)
            yield c


async def _noop():
    return None


class TestStageRunnerGovernor:
    async def test_default_llm_resource_acquire_release(self):
        """default_stream_llm 资源：acquire/release。"""
        governor = MockGovernor()
        chunks = [SourceChunk(text="正文"), SourceChunk(finish=True)]
        cfg = StageConfig(
            name="main",
            source_type=SourceType.LLM,
            source=_MockSource(chunks),
            resource=DEFAULT_STREAM_LLM_RESOURCE,
        )
        runner = StageRunner(cfg, DEFAULT_CONFIG, governor=governor)

        texts = []
        outcome = await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: _noop(),
            on_references=lambda r: _noop(),
            on_hit=lambda h: _noop(),
        )

        # verify acquire called
        assert len(governor.tokens) == 0  # 已释放
        # verify release called
        assert len(governor.released) == 1
        assert governor.released[0][0] == DEFAULT_STREAM_LLM_RESOURCE

    async def test_other_resource_acquire_release(self):
        """其他资源：stage 完成时释放。"""
        governor = MockGovernor()
        chunks = [SourceChunk(text="正文"), SourceChunk(finish=True)]
        cfg = StageConfig(
            name="vector",
            source_type=SourceType.LLM,
            source=_MockSource(chunks),
            resource="vector_search",
        )
        runner = StageRunner(cfg, DEFAULT_CONFIG, governor=governor)

        texts = []
        outcome = await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: _noop(),
            on_references=lambda r: _noop(),
            on_hit=lambda h: _noop(),
        )

        # verify release called
        assert len(governor.released) == 1
        assert governor.released[0][0] == "vector_search"

    async def test_no_resource_no_governor(self):
        """无资源/无 governor：不 acquire/release。"""
        governor = MockGovernor()
        chunks = [SourceChunk(text="正文"), SourceChunk(finish=True)]
        cfg = StageConfig(
            name="main",
            source_type=SourceType.LLM,
            source=_MockSource(chunks),
            resource=None,  # 无资源
        )
        runner = StageRunner(cfg, DEFAULT_CONFIG, governor=governor)

        texts = []
        outcome = await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: _noop(),
            on_references=lambda r: _noop(),
            on_hit=lambda h: _noop(),
        )

        # verify no acquire/release
        assert len(governor.tokens) == 0
        assert len(governor.released) == 0

    async def test_cancel_continues_consuming(self):
        """cancel 时继续消费 LLM 流（sem 不释放，直到 LLM 结束）。"""
        governor = MockGovernor()
        cancel_token = CancellationToken()
        chunks = [
            SourceChunk(text="第一段"),
            SourceChunk(text="第二段"),
            SourceChunk(finish=True),
        ]
        cfg = StageConfig(
            name="main",
            source_type=SourceType.LLM,
            source=_MockSource(chunks, delay=0.02),  # 每个 chunk 延迟 0.02s
            resource=DEFAULT_STREAM_LLM_RESOURCE,
        )
        runner = StageRunner(cfg, DEFAULT_CONFIG, governor=governor, cancel_token=cancel_token)

        texts = []

        async def cancel_after_first():
            # cancel after first chunk (before second chunk)
            import asyncio
            await asyncio.sleep(0.01)  # 在第一个 chunk 后，第二个 chunk 前
            cancel_token.cancel("test")

        import asyncio
        cancel_task = asyncio.create_task(cancel_after_first())

        outcome = await runner.run(
            on_text=lambda t: texts.append(t) or _noop(),
            on_thinking=lambda t: _noop(),
            on_references=lambda r: _noop(),
            on_hit=lambda h: _noop(),
        )

        await cancel_task

        # verify: continue consuming after cancel (release sem only after LLM ends)
        assert len(governor.released) == 1  # LLM 结束时释放
        # text after cancel not output (second paragraph after cancel, should not output)
        assert "第二段" not in "".join(texts)