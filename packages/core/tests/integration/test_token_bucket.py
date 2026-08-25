"""P2 验收测试：AsyncTokenBucket。"""
import asyncio
import time

import pytest

from orditect.core import AsyncTokenBucket, AcquireTimeoutError


@pytest.mark.integration
class TestTokenBucketBasics:
    """令牌桶基础行为。"""

    async def test_acquire_within_capacity(self, redis_client):
        """容量内获取成功。"""
        bucket = AsyncTokenBucket(
            redis_client,
            "test_bucket",
            capacity=3,
            refill_amount=1,
            refill_frequency=1.0,
        )

        # first 3 succeed immediately
        start = time.monotonic()
        await bucket.acquire(max_sleep=1.0)
        await bucket.acquire(max_sleep=1.0)
        await bucket.acquire(max_sleep=1.0)
        elapsed = time.monotonic() - start

        assert elapsed < 0.5  # 应该在 0.5 秒内完成

    async def test_acquire_beyond_capacity_waits(self, redis_client):
        """超出容量后需要等待 refill。"""
        bucket = AsyncTokenBucket(
            redis_client,
            "test_bucket_wait",
            capacity=2,
            refill_amount=1,
            refill_frequency=0.5,
        )

        # first 2 succeed immediately
        await bucket.acquire(max_sleep=5.0)
        await bucket.acquire(max_sleep=5.0)

        # 3rd needs ~0.5s wait
        start = time.monotonic()
        await bucket.acquire(max_sleep=5.0)
        elapsed = time.monotonic() - start

        assert 0.4 < elapsed < 0.7

    async def test_max_sleep_rejection(self, redis_client):
        """预估等待超 max_sleep 时拒绝。"""
        bucket = AsyncTokenBucket(
            redis_client,
            "test_bucket_reject",
            capacity=1,
            refill_amount=1,
            refill_frequency=5.0,  # 5 秒才补充 1 个
        )

        # 1st succeeds
        await bucket.acquire(max_sleep=1.0)

        # 2nd estimated wait 5s exceeds max_sleep=1.0, should reject
        with pytest.raises(AcquireTimeoutError):
            await bucket.acquire(max_sleep=1.0)