"""@limited 装饰器单元测试。"""
import asyncio

import pytest

from orditect.core import get_registry, limited, AcquireTimeoutError


class TestLimitedSemaphore:
    """@limited 装饰器（semaphore 模式）测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        registry = get_registry()
        registry.clear()

    async def test_limited_wait_mode(self, redis_client):
        """wait 模式：正常获取和释放。"""
        registry = get_registry()
        registry.register_semaphore("test_sem", redis_client, limit=1, lease_time=5.0)

        call_count = 0

        @limited(resource="test_sem", mode="wait", timeout=1.0)
        async def my_func():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await my_func()
        assert result == "ok"
        assert call_count == 1

    async def test_limited_reject_mode(self, redis_client):
        """reject 模式：拿不到立即拒绝。"""
        registry = get_registry()
        sem = registry.register_semaphore("test_sem", redis_client, limit=1, lease_time=5.0)

        # occupy first
        token = await sem.acquire(timeout=1.0)

        @limited(resource="test_sem", mode="reject")
        async def my_func():
            return "ok"

        # should be rejected
        with pytest.raises(AcquireTimeoutError, match="rejected"):
            await my_func()

        await sem.release(token)

    async def test_limited_timeout(self, redis_client):
        """wait 模式：超时抛 AcquireTimeoutError。"""
        registry = get_registry()
        sem = registry.register_semaphore("test_sem", redis_client, limit=1, lease_time=5.0)

        # occupy first
        token = await sem.acquire(timeout=1.0)

        @limited(resource="test_sem", mode="wait", timeout=0.3)
        async def my_func():
            return "ok"

        # should timeout
        with pytest.raises(AcquireTimeoutError, match="timeout"):
            await my_func()

        await sem.release(token)

    async def test_limited_auto_release(self, redis_client):
        """验证函数执行后自动释放。"""
        registry = get_registry()
        sem = registry.register_semaphore("test_sem", redis_client, limit=1, lease_time=5.0)

        @limited(resource="test_sem", mode="wait", timeout=1.0)
        async def my_func():
            return "ok"

        await my_func()

        # should have released, can acquire again
        assert await sem.in_use() == 0

    async def test_limited_exception_still_releases(self, redis_client):
        """函数抛异常时仍应释放。"""
        registry = get_registry()
        sem = registry.register_semaphore("test_sem", redis_client, limit=1, lease_time=5.0)

        @limited(resource="test_sem", mode="wait", timeout=1.0)
        async def my_func():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await my_func()

        # should have released
        assert await sem.in_use() == 0


class TestLimitedBucket:
    """@limited 装饰器（bucket 模式）测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        registry = get_registry()
        registry.clear()

    async def test_limited_bucket_wait(self, redis_client):
        """bucket wait 模式。"""
        registry = get_registry()
        registry.register_bucket(
            "test_bucket",
            redis_client,
            capacity=2,
            refill_amount=1,
            refill_frequency=0.5,
        )

        @limited(resource="test_bucket", resource_type="bucket", mode="wait")
        async def my_func():
            return "ok"

        # first 2 succeed immediately
        await my_func()
        await my_func()

        # 3rd needs to wait
        import time
        start = time.monotonic()
        await my_func()
        elapsed = time.monotonic() - start
        assert 0.4 < elapsed < 0.7

    async def test_limited_bucket_reject(self, redis_client):
        """bucket reject 模式。"""
        registry = get_registry()
        bucket = registry.register_bucket(
            "test_bucket",
            redis_client,
            capacity=1,
            refill_amount=1,
            refill_frequency=5.0,  # 5 秒才补充
        )

        # 1st succeeds
        await bucket.acquire(max_sleep=1.0)

        @limited(resource="test_bucket", resource_type="bucket", mode="reject")
        async def my_func():
            return "ok"

        # should be rejected
        with pytest.raises(AcquireTimeoutError, match="rejected"):
            await my_func()