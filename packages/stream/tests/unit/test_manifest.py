"""ManifestBuilder 单测：聚合 + ext + 持久化。"""
import pytest

from orditect.stream.enrich import PlaceholderRecord, PlaceholderRegistry
from orditect.stream.events import PlaceholderState
from orditect.stream.finalizer import ManifestBuilder
from orditect.stream.runner.types import StreamResult
from orditect.stream.stages import StageOutcome


class _MemoryStore:
    def __init__(self):
        self.saved = {}

    async def save(self, stream_id, manifest, ttl):
        self.saved[stream_id] = (manifest, ttl)

    async def get(self, stream_id):
        return self.saved.get(stream_id, (None,))[0]


class TestManifestBuilder:
    async def test_single_stream_flat_stages(self):
        store = _MemoryStore()
        builder = ManifestBuilder(store, result_ttl=100)

        sr = StreamResult(stream_id="s1")
        sr.stages["main"] = StageOutcome(content="正文", thinking="思考")

        reg = PlaceholderRegistry()
        await reg.register(PlaceholderRecord(
            placeholder_id="ph_1", stream_id="s1", stage="main",
            context_text="ctx", loading_url="l.jpg", task_ref="local:job_1",
            char_offset=6,
        ))
        await reg.mark_resolved("ph_1", "real.jpg")

        manifest = await builder.build({"s1": sr}, reg, usage={"input_tokens": 10})

        # single substream stages flattened
        assert "main" in manifest["stages"]
        assert manifest["stages"]["main"]["content"] == "正文"
        assert manifest["stages"]["main"]["thinking"] == "思考"
        # placeholder resolved (P0: with char_offset + stage)
        ph = manifest["placeholders"][0]
        assert ph["state"] == "resolved"
        assert ph["url"] == "real.jpg"
        assert ph["task_ref"] == "local:job_1"
        assert ph["stage"] == "main"
        assert ph["char_offset"] == 6
        # persistence
        assert "s1" in store.saved

    async def test_multi_stream_grouped_stages(self):
        store = _MemoryStore()
        builder = ManifestBuilder(store)
        sr1 = StreamResult(stream_id="s1")
        sr1.stages["main"] = StageOutcome(content="A")
        sr2 = StreamResult(stream_id="s2")
        sr2.stages["main"] = StageOutcome(content="B")
        reg = PlaceholderRegistry()

        manifest = await builder.build({"s1": sr1, "s2": sr2}, reg)
        # multi substream grouped by stream_id
        assert "s1:main" in manifest["stages"]
        assert "s2:main" in manifest["stages"]

    async def test_finalizer_hook_ext(self):
        store = _MemoryStore()

        async def my_hook(stream_results, registry):
            return {"category": 3, "suggestion": ["a", "b"]}

        builder = ManifestBuilder(store, hooks=[my_hook])
        sr = StreamResult(stream_id="s1")
        sr.stages["main"] = StageOutcome(content="正文")
        reg = PlaceholderRegistry()

        manifest = await builder.build({"s1": sr}, reg)
        assert manifest["ext"]["category"] == 3
        assert manifest["ext"]["suggestion"] == ["a", "b"]

    async def test_pending_placeholder_in_manifest(self):
        store = _MemoryStore()
        builder = ManifestBuilder(store)
        sr = StreamResult(stream_id="s1")
        sr.stages["main"] = StageOutcome(content="正文")
        reg = PlaceholderRegistry()
        await reg.register(PlaceholderRecord(
            placeholder_id="ph_9", stream_id="s1", stage="main",
            context_text="ctx", loading_url="l.jpg", task_ref="tf:task-abc",
            char_offset=10,
        ))
        # not resolve, keep pending

        manifest = await builder.build({"s1": sr}, reg)
        ph = manifest["placeholders"][0]
        assert ph["state"] == "pending"
        assert ph["task_ref"] == "tf:task-abc"
        assert ph["stage"] == "main"
        assert ph["char_offset"] == 10
        assert ph.get("url") is None          # url 为 None 时键被省略
        assert "url" not in ph                # 或直接断言键不存在