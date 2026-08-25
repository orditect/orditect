"""v0.3.2 钉扎：索引 ZSET 租约化 + initialize 原子幂等 + 批次 1 修复验收。"""
import asyncio
import json
import time

import pytest

from orditect.core import (
    AsyncLeaseSemaphore,
    AsyncTokenBucket,
    AdmissionQuotaRedisDB,
    RedisDB,
    TaskRedisDB,
)


@pytest.mark.pinning
class TestIndexLeaseModel:
    """簇 A：索引成员级租约（#8 验收 + T9 翻转）。"""

    async def test_long_ttl_member_survives_short_ttl_sibling(self, redis_url, redis_client):
        """#8 核心验收：共享索引下长 TTL 活跃成员不被短 TTL 成员连坐。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("long", expiry=100)
        await db.initialize_task("short", expiry=1)

        await asyncio.sleep(1.5)

        ids = await db.list_task_ids_by_status("pending")
        assert "long" in ids, "活跃成员被短 TTL 兄弟连坐失踪（v0.3.1 SET+TTL 模型的病）"
        assert "short" not in ids
        await db.close()

    async def test_expired_member_lazy_cleaned_on_read(self, redis_url, redis_client):
        """T9 翻转：过期成员读路径惰性清理（不再依赖 key 整体蒸发）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_expire", expiry=1)
        assert "t_expire" in await db.list_task_ids_by_status("pending")

        await asyncio.sleep(1.5)

        assert await db.get_task("t_expire") == {}
        assert "t_expire" not in await db.list_task_ids_by_status("pending")
        await db.close()

    async def test_children_index_lazy_cleanup(self, redis_url, redis_client):
        """谱系索引同模型：子任务过期后读路径不可见，父主记录存活。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("parent_co", expiry=100)
        await db.initialize_task("child_co", expiry=1, parent_task_id="parent_co")
        assert await db.list_children("parent_co") == ["child_co"]

        await asyncio.sleep(1.5)

        assert await db.list_children("parent_co") == []
        parent = await db.get_task("parent_co")
        assert parent["status"] == "pending"
        await db.close()

    async def test_update_refreshes_index_lease(self, redis_url, redis_client):
        """update_task 重设主记录 EX 时，索引租约同步推进。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_renew", expiry=2)
        await asyncio.sleep(1)
        await db.update_task("t_renew", {"status": "in_progress"}, expiry=100)

        await asyncio.sleep(1.2)  # 已超过初始 expiry=2

        assert "t_renew" in await db.list_task_ids_by_status("in_progress")
        await db.close()


@pytest.mark.pinning
class TestAtomicIdempotentInit:
    """#14：initialize_task Lua 原子幂等。"""

    async def test_concurrent_if_not_exists_exactly_one_wins(self, redis_url):
        """并发双初始化：恰好一个成功（Lua EXISTS+写原子化）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        results = await asyncio.gather(
            db.initialize_task("t14_race", if_not_exists=True),
            db.initialize_task("t14_race", if_not_exists=True),
        )
        assert sorted(results) == [False, True]
        await db.close()

    async def test_no_overwrite_after_status_progression(self, redis_url):
        """状态推进后重入：不抹回（原 TOCTOU 窗口已封死）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t14", if_not_exists=True)
        await db.update_task("t14", {"status": "in_progress"})

        assert await db.initialize_task("t14", if_not_exists=True) is False
        assert (await db.get_task("t14"))["status"] == "in_progress"
        await db.close()


@pytest.mark.pinning
class TestBatch1Fixes:
    """#1 / #6 / #10 / #15 / #21 验收。"""

    async def test_update_works_in_di_mode(self, redis_client):
        """#1：DI 模式 update() 可用（lazy 注册，connect() no-op 也无碍）。"""
        db = RedisDB(client=redis_client)
        await db.connect()  # no-op
        await db.set_with_expiry("k_di", {"a": 1})
        await db.update("k_di", {"b": 2})
        assert json.loads(await db.get("k_di")) == {"a": 1, "b": 2}

    async def test_acquire_with_fractional_lease(self, redis_client):
        """#6：小数 lease 不炸（EXPIRE 整数化）。"""
        sem = AsyncLeaseSemaphore(redis_client, "frac_lease", limit=1, lease_time=0.7)
        token = await sem.acquire(timeout=1.0)
        await sem.release(token)

    async def test_watchdog_self_deregisters_on_expiry(self, redis_client):
        """#10：watchdog 发现 token 被回收后自动摘除 _guards 登记。"""
        sem = AsyncLeaseSemaphore(
            redis_client, "dereg", limit=1, lease_time=1.0, renew_interval=0.2
        )
        token = await sem.acquire(timeout=1.0)
        assert token.value in sem._guards

        await redis_client.zrem(sem.key, token.value)  # 模拟外部回收

        await asyncio.sleep(0.6)  # 等续约发现 0 → 退出 → finally 摘除
        assert token.value not in sem._guards

    async def test_bucket_wait_uses_server_clock(self, redis_client):
        """#15：返回的等待时长基于服务端时钟，与实际 sleep 一致。"""
        bucket = AsyncTokenBucket(
            redis_client, "srv_clock", capacity=1, refill_amount=1, refill_frequency=0.5
        )
        await bucket.acquire(max_sleep=1.0)

        start = time.monotonic()
        waited = await bucket.acquire(max_sleep=2.0)
        elapsed = time.monotonic() - start

        assert 0.3 < waited < 0.8
        assert abs(waited - elapsed) < 0.3

    async def test_pending_key_has_ttl(self, redis_url, redis_client):
        """#21：reserve 后 pending_key 带 TTL（兜底回收）。"""
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        await db.reserve_units(
            scope="ttl_check", task_id="t1", units=5, max_units=10, task_ttl_sec=100
        )
        ttl = await redis_client.ttl("admission:ttl_check:pending_units")
        assert 0 < ttl <= 200

        # TTL preserved after release (not cleared by SET)
        await db.release_units(scope="ttl_check", task_id="t1")
        ttl2 = await redis_client.ttl("admission:ttl_check:pending_units")
        assert ttl2 > 0
        await db.close()

    async def test_quota_reserve_zero_units_allowed(self, redis_url):
        """簇 C 预埋：units=0 合法（建账语义，不占额度）。"""
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        result = await db.reserve_units(
            scope="ledger_open", task_id="__ledger__", units=0, max_units=1000
        )
        assert result["ok"] is True
        assert result["current"] == 0
        await db.close()