"""pytest 配置和 fixtures"""
import sys
from pathlib import Path

# ensure src/ is in sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# flow tests also need sibling package sources on a fresh clone (no
# pip-installed packages): fake_infra imports orditect.protocol, and the
# integration suite imports orditect.core. Mirrors the per-package conftest
# pattern used by bridge-openai / adapter-ui (v0.1.7, issue #7).
_REPO = Path(__file__).resolve().parents[3]
for _pkg in ("core", "protocol"):
    _src = _REPO / "packages" / _pkg / "src"
    if _src.is_dir() and str(_src) not in sys.path:
        sys.path.insert(0, str(_src))

import os
import pytest
import redis.asyncio as aioredis

REDIS_URL = os.getenv("FTF_TEST_REDIS_URL", "redis://127.0.0.1:6379/15")


async def _available(url: str) -> bool:
    """检查 Redis 是否可用"""
    try:
        c = aioredis.from_url(url, decode_responses=True)
        await c.ping()
        await c.aclose()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
async def redis_url() -> str:
    """Redis 连接地址（session 级，检查可用性）"""
    if not await _available(REDIS_URL):
        pytest.skip(f"Redis 不可用: {REDIS_URL}")
    return REDIS_URL


@pytest.fixture
async def redis_client(redis_url):
    """Redis 客户端（函数级，跑前清场）"""
    client = aioredis.from_url(redis_url, decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()