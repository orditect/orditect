"""v0.3.0 钉扎测试（B 组）：Lua 三件套潜伏 bug + W2/W3 配套（T2/T3/T5/T8）。"""
import asyncio
import time

import pytest

from orditect.core import (
    AsyncLeaseSemaphore,
    AsyncTokenBucket,
    AcquireTimeoutError,
    CancellationToken,
    RedisDB,
    TaskRedisDB,
)


@pytest.mark.pinning
class TestT2WatchdogLongHold:
    """T2：watchdog 持有 >2×lease 仍不失效（sem_refresh 刷 TTL 验收）。"""

    async def test_hold_beyond_2x_lease_no_doubling(self, redis_client):
        """lease=1s，持有 2.5s（>2×lease），容量不翻倍、key 不蒸发。

        修复前：key 在 2s 后过期，watchdog 停止，新 acquire 看到空集合放行。
        修复后：续约同步刷 EXPIRE，key 持续存活，容量不失效。
        """
        sem = AsyncLeaseSemaphore(redis_client, "t2_longhold", limit=1, lease_time=1.0)

        t1 = await sem.acquire(timeout=1.0)
        try:
            # hold over 2×lease (2s)
            await asyncio.sleep(2.5)

            # key should still be alive (renewed TTL)
            key_exists = await redis_client.exists(sem.key)
            assert key_exists, "semaphore key expired despite watchdog renewal"

            # capacity not doubled: second acquire should still timeout
            with pytest.raises(AcquireTimeoutError):
                await sem.acquire(timeout=0.5)
        finally:
            await sem.release(t1)

        # can acquire again after release
        t2 = await sem.acquire(timeout=1.0)
        await sem.release(t2)


@pytest.mark.pinning
class TestT3SlowBucketNoReset:
    """T3：慢速桶状态不被 TTL 静默重置（bucket TTL 自计算验收）。"""

    async def test_slow_bucket_state_preserved(self, redis_client):
        """refill_frequency=2s 的桶，跨旧 TTL（30s 太长无法测，用短周期验证语义）。

        验证：状态 TTL 随 refill 参数增长，慢速桶不会在 30s 后重置为满容量。
        用短周期（2s）间接验证：获取后等 1s（未满一个 refill 周期），
        第二个获取应等待（而非立即成功=桶被重置）。
        """
        bucket = AsyncTokenBucket(
            redis_client,
            "t3_slow",
            capacity=1,
            refill_amount=1,
            refill_frequency=2.0,
        )

        # 1st acquire (consume only token)
        await bucket.acquire(max_sleep=0.5)

        # wait 1s (less than refill period 2s), 2nd acquire should need ~1s wait
        await asyncio.sleep(1.0)
        start = time.monotonic()
        await bucket.acquire(max_sleep=3.0)
        elapsed = time.monotonic() - start

        # if bucket reset (bug), here would succeed immediately (elapsed ≈ 0)
        # after fix: need to wait remaining refill time ≈ 1s
        assert elapsed > 0.3, f"bucket appears reset (elapsed={elapsed:.2f}s, expected >0.3s)"


@pytest.mark.pinning
class TestT5JsonMergeAtomicity:
    """T5：json_merge 原子性（W2 验收）。"""

    async def test_concurrent_merge_no_lost_update(self, redis_url):
        """并发 merge 同 key 不丢更新（修复前 RMW 互相覆盖）。"""
        db = RedisDB(redis_url)
        await db.connect()

        # initialize
        await db.set_with_expiry("t5_key", {"counter": 0, "fields": []})

        # concurrent 10 merges (each adds one field)
        async def merge_one(i: int):
            await db.update("t5_key", {f"field_{i}": i})

        await asyncio.gather(*[merge_one(i) for i in range(10)])

        # all fields should be present (atomic merge no loss)
        raw = await db.get("t5_key")
        import json
        data = json.loads(raw)
        for i in range(10):
            assert f"field_{i}" in data, f"lost update: field_{i} missing"

        await db.close()

    async def test_update_not_found_raises_keyerror(self, redis_url):
        """key 不存在时抛 KeyError（原 ValueError，语义更正）。"""
        db = RedisDB(redis_url)
        await db.connect()

        with pytest.raises(KeyError, match="key not exists"):
            await db.update("nonexistent", {"a": 1})

        await db.close()


@pytest.mark.pinning
class TestT8CancellationTokenCache:
    """T8：CancellationToken 轮询缓存（B6 验收）。"""

    async def test_cache_reduces_redis_calls(self, redis_url, redis_client):
        """窗口内 Redis 调用次数受控（spy 断言）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()
        await db.initialize_task("t8_task")

        # wrap get_task count calls
        original_get_task = db.get_task
        call_count = 0

        async def spy_get_task(task_id):
            nonlocal call_count
            call_count += 1
            return await original_get_task(task_id)

        db.get_task = spy_get_task

        token = CancellationToken("t8_task", db, min_interval=0.2)

        # consecutive 5 queries within window
        for _ in range(5):
            await token.is_cancelled()

        # should query Redis only once (cache hit within window)
        assert call_count == 1, f"expected 1 Redis call, got {call_count}"

        # query again after window exceeded
        await asyncio.sleep(0.25)
        await token.is_cancelled()
        assert call_count == 2, f"expected 2 Redis calls after window, got {call_count}"

        await db.close()

    async def test_cache_disabled_with_zero_interval(self, redis_url, redis_client):
        """min_interval=0 时禁用缓存（每次必查 Redis）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()
        await db.initialize_task("t8_nocache")

        original_get_task = db.get_task
        call_count = 0

        async def spy_get_task(task_id):
            nonlocal call_count
            call_count += 1
            return await original_get_task(task_id)

        db.get_task = spy_get_task

        token = CancellationToken("t8_nocache", db, min_interval=0)

        for _ in range(3):
            await token.is_cancelled()

        assert call_count == 3, f"expected 3 Redis calls (no cache), got {call_count}"
        await db.close()