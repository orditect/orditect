"""S2 pinning: ProtocolResultStore relocation adapter.

- ttl seconds -> absolute expire_at conversion (T1/T7).
- get returns None once expired (lazy expiry, T1).
- backward-compatible ResultStoreProtocol signature (ttl seconds).
- runs over the open orditect-adapter-memory result part.
"""
import asyncio

import pytest

from orditect.adapter.memory import MemoryStore
from orditect.stream.store import ProtocolResultStore, get_protocol_store


@pytest.mark.unit
class TestProtocolResultStore:
    async def test_save_and_get_round_trip(self):
        parts = MemoryStore()
        store = ProtocolResultStore(parts.result, parts.result)

        manifest = {"stages": {"main": {"content": "正文"}}, "placeholders": []}
        await store.save("s1", manifest, ttl=100)
        got = await store.get("s1")
        assert got is not None
        assert got["stages"]["main"]["content"] == "正文"

    async def test_ttl_expiry_returns_none(self):
        """T1: record past its ttl is invisible (lazy expiry)."""
        parts = MemoryStore()
        store = ProtocolResultStore(parts.result, parts.result)

        await store.save("s_exp", {"k": "v"}, ttl=1)
        assert await store.get("s_exp") is not None
        await asyncio.sleep(1.1)
        assert await store.get("s_exp") is None

    async def test_get_missing_returns_none(self):
        parts = MemoryStore()
        store = ProtocolResultStore(parts.result, parts.result)
        assert await store.get("ghost") is None

    async def test_factory_entry_point(self):
        parts = MemoryStore()
        store = get_protocol_store(parts.result, parts.result)
        await store.save("s_factory", {"a": 1}, ttl=100)
        assert (await store.get("s_factory"))["a"] == 1

    async def test_zero_ttl_means_no_expiry(self):
        """ttl<=0 -> far-future expire_at (no expiry), record persists."""
        parts = MemoryStore()
        store = ProtocolResultStore(parts.result, parts.result)
        await store.save("s_noexp", {"k": "v"}, ttl=0)
        await asyncio.sleep(0.05)
        assert await store.get("s_noexp") is not None