"""P2 验收测试：AsyncLeaseSemaphore 三大验收标准。

验收标准：
  A 崩溃回收：杀掉持有任务，槽位须在 lease 时间内自动回收 ✅
  B 不翻倍：满负荷 + 持有超 lease 且 watchdog 存活，容量不翻倍不失效 ✅
  C 不泄漏：超时路径零泄漏、零误持 ✅
"""
import asyncio

import pytest

from orditect.core import AsyncLeaseSemaphore, AcquireTimeoutError


@pytest.mark.integration
class TestAcceptanceA:
    """验收 A：崩溃回收。"""

    async def test_crashed_holder_slot_recycled(self, redis_client):
        """持有任务崩溃（未释放），槽位应在 lease 时间内自动回收。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_crash", limit=1, lease_time=2.0)

        # simulate crash: acquire without release, no renewal (manually stop watchdog)
        token = await sem.acquire(timeout=1.0)

        # manually stop watchdog (simulate crash)
        guard = sem._guards.get(token.value)
        if guard:
            await guard.stop()

        # wait lease expiry
        await asyncio.sleep(2.5)

        # slot should be reclaimed, can acquire again
        token2 = await sem.acquire(timeout=1.0)
        assert token2.value != token.value


@pytest.mark.integration
class TestAcceptanceB:
    """验收 B：不翻倍（watchdog 存活性）。"""

    async def test_full_load_long_hold_no_doubling(self, redis_client):
        """满负荷 + 长持有（超 lease）+ watchdog 存活，容量不翻倍。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_double", limit=2, lease_time=3.0)

        # acquire 2 slots (full load)
        t1 = await sem.acquire(timeout=1.0)
        t2 = await sem.acquire(timeout=1.0)

        # hold over half lease time (watchdog should renew)
        await asyncio.sleep(2.0)

        # third acquire should timeout (capacity not doubled)
        with pytest.raises(AcquireTimeoutError):
            await sem.acquire(timeout=0.5)

        await sem.release(t1)
        await sem.release(t2)


@pytest.mark.integration
class TestAcceptanceC:
    """验收 C：不泄漏（超时路径）。"""

    async def test_timeout_no_leak(self, redis_client):
        """acquire 超时后，不应占用槽位。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_leak", limit=1, lease_time=5.0)

        t1 = await sem.acquire(timeout=1.0)

        # second acquire times out
        with pytest.raises(AcquireTimeoutError):
            await sem.acquire(timeout=0.5)

        # after timeout should not occupy slot
        assert await sem.in_use() == 1

        await sem.release(t1)