"""测试基座：真实 Redis（Lua 是核心资产，不用 fakeredis 自欺）。

默认连 db15，跑前 flushdb，与业务数据物理隔离。
环境变量 FTB_TEST_REDIS_URL 可覆盖默认地址。
"""

import sys
from pathlib import Path

# ensure src/ is in sys.path (pytest loads conftest before pythonpath config takes effect)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import os

import pytest
import redis.asyncio as aioredis

REDIS_URL = os.getenv("FTB_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


async def _available(url: str) -> bool:
    """检查 Redis 是否可用。"""
    try:
        c = aioredis.from_url(url, decode_responses=True)
        await c.ping()
        await c.aclose()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
async def redis_url() -> str:
    """Redis 连接地址（session 级，检查可用性）。"""
    if not await _available(REDIS_URL):
        pytest.skip(f"Redis 不可用: {REDIS_URL}（pinning/integration/chaos 均需要）")
    return REDIS_URL


@pytest.fixture
async def redis_client(redis_url):
    """Redis 客户端（函数级，跑前清场）。"""
    client = aioredis.from_url(redis_url, decode_responses=True)
    await client.flushdb()  # db15 专用，跑前清场
    yield client
    await client.aclose()