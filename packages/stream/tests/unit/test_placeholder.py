"""PlaceholderRegistry 状态机单测。"""
import asyncio

import pytest

from orditect.stream.enrich import PlaceholderRecord, PlaceholderRegistry
from orditect.stream.events import PlaceholderState


def _rec(pid="ph_1", sid="s1"):
    return PlaceholderRecord(
        placeholder_id=pid, stream_id=sid, stage="main",
        context_text="ctx", loading_url="loading.jpg",
    )


class TestPlaceholderRegistry:
    async def test_register_and_pending(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec())
        assert len(reg.pending()) == 1
        assert reg.get("ph_1").state is PlaceholderState.PENDING

    async def test_mark_resolved(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec())
        rec = await reg.mark_resolved("ph_1", "real.jpg")
        assert rec.state is PlaceholderState.RESOLVED
        assert rec.url == "real.jpg"
        assert rec.elapsed() is not None
        assert len(reg.pending()) == 0

    async def test_mark_failed(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec())
        rec = await reg.mark_failed("ph_1", "boom", "fb.jpg")
        assert rec.state is PlaceholderState.FAILED
        assert rec.error == "boom"
        assert rec.fallback_url == "fb.jpg"

    async def test_wait_one_resolved(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec())

        async def later():
            await asyncio.sleep(0.05)
            await reg.mark_resolved("ph_1", "real.jpg")

        task = asyncio.create_task(later())
        rec = await reg.wait_one("ph_1", timeout=1.0)
        await task
        assert rec.state is PlaceholderState.RESOLVED

    async def test_wait_one_timeout(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec())
        rec = await reg.wait_one("ph_1", timeout=0.05)
        assert rec.state is PlaceholderState.PENDING  # 超时仍 pending

    async def test_wait_all(self):
        reg = PlaceholderRegistry()
        await reg.register(_rec("ph_1"))
        await reg.register(_rec("ph_2"))

        async def later():
            await asyncio.sleep(0.05)
            await reg.mark_resolved("ph_1", "a.jpg")
            await reg.mark_resolved("ph_2", "b.jpg")

        task = asyncio.create_task(later())
        all_rec = await reg.wait_all(timeout=1.0)
        await task
        assert all(r.state is PlaceholderState.RESOLVED for r in all_rec)