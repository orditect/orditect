"""混沌测试：Redis 重启（P2 落地后启用）。"""
import asyncio

import pytest

from orditect.core import AsyncLeaseSemaphore


@pytest.mark.chaos
class TestRedisRestart:
    """Redis 重启场景。"""

    @pytest.mark.skip(reason="P2 落地后启用，需手动重启 Redis")
    async def test_redis_restart_recovery(self, redis_client):
        """Redis 重启后，信号量应恢复正常。"""
        sem = AsyncLeaseSemaphore(redis_client, "chaos_restart", limit=2, lease_time=5.0)

        t1 = await sem.acquire(timeout=1.0)

        # manually restart Redis (manual operation during test)
        print("请手动重启 Redis，然后按回车继续...")
        input()

        # should continue working after restart
        t2 = await sem.acquire(timeout=1.0)
        assert t2 is not None