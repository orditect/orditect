"""taskflow 适配集成测试（需 Redis，不可用则 skip）。

运行条件：
- orditect-flow 已安装
- Redis 可用（FTS_TEST_REDIS_URL 环境变量，默认 redis://127.0.0.1:6379/15）
"""
import os

import pytest

pytestmark = pytest.mark.integration

REDIS_URL = os.getenv("FTS_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


async def _redis_available() -> bool:
    try:
        import redis.asyncio as aioredis
        c = aioredis.from_url(REDIS_URL, decode_responses=True)
        await c.ping()
        await c.aclose()
        return True
    except Exception:
        return False


def _taskflow_installed() -> bool:
    try:
        import orditect.flow  # noqa
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _taskflow_installed(), reason="orditect-flow 未安装")
class TestTaskflowAdapter:
    async def test_taskflow_result_store(self):
        if not await _redis_available():
            pytest.skip(f"Redis 不可用: {REDIS_URL}")

        import redis.asyncio as aioredis
        from orditect.flow import get_default_storage
        from orditect.stream.adapters.taskflow import TaskflowResultStore

        client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await client.flushdb()

        storage = get_default_storage(client)
        if hasattr(storage, "connect"):
            await storage.connect()

        store = TaskflowResultStore(storage)
        manifest = {"stages": {"main": {"content": "正文"}}, "placeholders": []}
        await store.save("s_test", manifest, ttl=100)

        got = await store.get("s_test")
        assert got is not None
        assert got["stages"]["main"]["content"] == "正文"

        await client.aclose()