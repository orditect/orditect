"""P1 行为钉扎测试：AsyncLeaseSemaphore。

P2 手术完成后的变化：
- test_expired_token_recycled：P1 钉扎过期回收，P2 修复后 watchdog 续租，不再过期
"""
import asyncio

import pytest

from orditect.core import AsyncLeaseSemaphore, AcquireTimeoutError


@pytest.mark.pinning
class TestSemaphoreBasics:
    """信号量基础行为钉扎。"""

    async def test_acquire_and_release(self, redis_client):
        """获取 + 释放。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_basic", limit=2, lease_time=5.0)

        token = await sem.acquire(timeout=1.0)
        assert token.resource == "test_basic"
        assert token.value

        await sem.release(token)

        # can acquire again after release
        token2 = await sem.acquire(timeout=1.0)
        await sem.release(token2)

    async def test_limit_enforced(self, redis_client):
        """并发上限被强制执行。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_limit", limit=2, lease_time=5.0)

        t1 = await sem.acquire(timeout=1.0)
        t2 = await sem.acquire(timeout=1.0)

        # third acquire should timeout
        with pytest.raises(AcquireTimeoutError):
            await sem.acquire(timeout=0.5)

        await sem.release(t1)
        await sem.release(t2)

    async def test_try_acquire(self, redis_client):
        """非阻塞获取。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_try", limit=1, lease_time=5.0)

        t1 = await sem.try_acquire()
        assert t1 is not None

        # full, try_acquire should return None
        t2 = await sem.try_acquire()
        assert t2 is None

        await sem.release(t1)

    async def test_in_use(self, redis_client):
        """水位查询。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_inuse", limit=3, lease_time=5.0)

        assert await sem.in_use() == 0

        t1 = await sem.acquire(timeout=1.0)
        assert await sem.in_use() == 1

        t2 = await sem.acquire(timeout=1.0)
        assert await sem.in_use() == 2

        await sem.release(t1)
        assert await sem.in_use() == 1

        await sem.release(t2)
        assert await sem.in_use() == 0


@pytest.mark.pinning
class TestLeaseExpiry:
    """租约过期行为（P2 修复后：watchdog 续租，不再过期）。"""

    async def test_watchdog_prevents_expiry(self, redis_client):
        """P2 修复后：watchdog 续租，持有期间不会过期。"""
        sem = AsyncLeaseSemaphore(redis_client, "test_expiry", limit=1, lease_time=2.0)

        t1 = await sem.acquire(timeout=1.0)

        # wait more than half lease time (watchdog should renew)
        await asyncio.sleep(1.5)

        # after P2 fix: t1's slot renewed by watchdog, not reclaimed
        # attempt to acquire second slot should timeout (capacity not doubled)
        with pytest.raises(AcquireTimeoutError):
            await sem.acquire(timeout=0.5)

        await sem.release(t1)

        # can acquire again after release
        t2 = await sem.acquire(timeout=1.0)
        await sem.release(t2)

@pytest.mark.pinning
class TestHoldReleaseRuntimeErrorFallback:
    """v0.1.7 pinning (issue #5): SemaphoreHold.__aexit__ degrades
    explicitly when create_task is unavailable (loop-teardown window):
    close the coroutine, log, skip — never raise out of the exit path."""

    async def test_hold_exit_with_create_task_unavailable(
        self, redis_client, monkeypatch, caplog
    ):
        import logging

        sem = AsyncLeaseSemaphore(redis_client, "hold_fallback", limit=1, lease_time=5.0)

        hold = sem.hold()
        token = await hold.__aenter__()

        def boom(coro, **kwargs):
            raise RuntimeError("simulated loop teardown")

        # Patch AFTER __aenter__: the watchdog start needs the real
        # create_task; only __aexit__ runs in the patched window.
        monkeypatch.setattr(asyncio, "create_task", boom)
        with caplog.at_level(logging.WARNING):
            await hold.__aexit__(None, None, None)

        assert any(
            "release skipped" in r.message for r in caplog.records
        )

        # The release was skipped in the simulated teardown window; release
        # manually for test isolation.
        monkeypatch.undo()
        await sem.release(token)
        assert await sem.in_use() == 0