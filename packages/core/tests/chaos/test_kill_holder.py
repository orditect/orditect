"""混沌测试：杀掉持有者（P2 落地后启用）。"""
import asyncio

import pytest

from orditect.core import AsyncLeaseSemaphore


@pytest.mark.chaos
class TestKillHolder:
    """持有任务崩溃场景。"""

    @pytest.mark.skip(reason="P2 watchdog 实现后启用")
    async def test_kill_holder_slot_recycled(self, redis_client):
        """杀掉持有协程（不释放），槽位应在 lease 时间内回收。"""
        sem = AsyncLeaseSemaphore(redis_client, "chaos_kill", limit=1, lease_time=2.0)

        async def holder():
            token = await sem.acquire(timeout=1.0)
            await asyncio.sleep(100)  # 永眠，模拟崩溃
            # never executes release

        # start holder
        task = asyncio.create_task(holder())
        await asyncio.sleep(0.2)  # 确保持有

        # kill holder (simulate crash)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # wait lease expiry
        await asyncio.sleep(2.5)

        # slot should be reclaimed
        token2 = await sem.acquire(timeout=1.0)
        assert token2 is not None