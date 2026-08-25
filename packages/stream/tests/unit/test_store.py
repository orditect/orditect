"""MemoryResultStore 单测。"""
import asyncio

import pytest

from orditect.stream.store import MemoryResultStore


class TestMemoryResultStore:
    async def test_save_and_get(self):
        store = MemoryResultStore()
        await store.save("s1", {"a": 1}, ttl=100)
        assert await store.get("s1") == {"a": 1}

    async def test_ttl_expiry(self):
        store = MemoryResultStore()
        await store.save("s1", {"a": 1}, ttl=1)
        await asyncio.sleep(1.1)
        assert await store.get("s1") is None

    async def test_get_missing(self):
        store = MemoryResultStore()
        assert await store.get("nope") is None