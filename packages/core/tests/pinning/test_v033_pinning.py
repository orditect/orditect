"""v0.3.3 钉扎：信号量生命周期完整性（S 簇）+ 数据面健壮性（B 簇）。"""
import asyncio
import logging
import time

import pytest

from orditect.core import (
    AdmissionQuotaRedisDB,
    AsyncLeaseSemaphore,
    AcquireTimeoutError,
    TaskRedisDB,
    get_pool_manager,
    get_registry,
)
from orditect.core.limiter.lease import LeaseGuard


@pytest.mark.pinning
class TestS1HoldConcurrencySafety:
    """S1：hold() 并发安全 + 裸 async with 已删除。"""

    def test_bare_async_with_removed(self, redis_client):
        """裸 async with sem 已物理删除（沿用 reconnect 删除先例）。"""
        sem = AsyncLeaseSemaphore(redis_client, "s1_removed", limit=2, lease_time=5.0)
        assert not hasattr(sem, "__aenter__"), "use sem.hold() instead"
        assert not hasattr(sem, "__aexit__")

    async def test_concurrent_hold_no_cross_release(self, redis_client):
        """两协程并发 hold：token 各自独立，互不误释放、容量不翻倍。

        修复前（_ctx_token 实例属性）：A 的 __aexit__ 释放 B 的 token——
        B 持期间第三个 acquire 能进（超发）；A 的槽位被 watchdog 永久续租。

        时序说明：acquire 有退避重试（50ms 起），释放后第一拍重试即命中——
        因此超发断言用 try_acquire（单拍，不重试），在 task_a 收尾后立即探测。
        """
        sem = AsyncLeaseSemaphore(redis_client, "s1_conc", limit=2, lease_time=5.0)
        gate_a = asyncio.Event()
        gate_b = asyncio.Event()

        async def holder(gate: asyncio.Event):
            async with sem.hold():
                await gate.wait()

        task_a = asyncio.create_task(holder(gate_a))
        task_b = asyncio.create_task(holder(gate_b))

        # wait until both coroutines complete acquire (water level=2 confirms, no longer rely on fixed sleep guesses)
        for _ in range(100):
            if await sem.in_use() == 2:
                break
            await asyncio.sleep(0.02)
        assert await sem.in_use() == 2, "two holders should both have acquired"

        # A exits first: released must be A's own token
        gate_a.set()
        await task_a  # task_a done ⇒ 其 __aexit__ 的 shield(release) 已落 Redis

        # B still holds → at this moment there should be exactly one free slot (A's released).
        # Use try_acquire single-shot probe to avoid timing ambiguity of acquire backoff retry.
        token_probe = await sem.try_acquire()
        assert token_probe is not None, "A's slot should be freed exactly once"

        # Pre-fix determination: if A mistakenly releases B's token, then free slots = 2 (A's leaked slot + B's wrongly released slot),
        # the second try_acquire would get B's slot — here must be None (B still holds, only 1 free slot).
        token_overflow = await sem.try_acquire()
        assert token_overflow is None, (
            "capacity doubled: A's exit wrongly released B's token (S1 regression)"
        )

        # return probe slot, let B exclusive
        await sem.release(token_probe)

        # add a holder to fill capacity (with B total 2 slots), then verify third acquire times out —
        # this is the valid determination of "capacity not doubled" (previously with limit=2 releasing 1 slot then acquire
        # should succeed, timeout assertion itself was invalid).
        gate_c = asyncio.Event()

        async def holder_c():
            async with sem.hold():
                await gate_c.wait()

        task_c = asyncio.create_task(holder_c())
        for _ in range(100):
            if await sem.in_use() == 2:
                break
            await asyncio.sleep(0.02)
        assert await sem.in_use() == 2, "capacity should be full again (B + C)"

        # capacity full (B + C) → third acquire must timeout
        with pytest.raises(AcquireTimeoutError):
            await sem.acquire(timeout=0.3)

        gate_c.set()
        await task_c

        gate_b.set()
        await task_b

        # after all released no leak, new round can acquire normally
        assert await sem.in_use() == 0
        token = await sem.acquire(timeout=1.0)
        await sem.release(token)

    async def test_hold_release_survives_outer_cancel(self, redis_client):
        """S2：cancel 打在 hold 块内，release 仍完成（shield 验收）。"""
        sem = AsyncLeaseSemaphore(redis_client, "s1_shield", limit=1, lease_time=5.0)

        async def holder():
            async with sem.hold():
                await asyncio.sleep(10)

        task = asyncio.create_task(holder())
        await asyncio.sleep(0.2)  # 等 acquire
        assert await sem.in_use() == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # release inside shield is independent task, wait for it
        for _ in range(50):
            if await sem.in_use() == 0:
                break
            await asyncio.sleep(0.02)
        assert await sem.in_use() == 0, "release swallowed by outer cancel"


@pytest.mark.pinning
class TestS3AcquireFailureRollback:
    """S3：post-Lua 步骤失败兜底归还槽位。"""

    async def test_guard_start_failure_releases_slot(self, redis_client, monkeypatch):
        """guard.start 抛错 → acquire 传播异常，但槽位已归还。"""
        sem = AsyncLeaseSemaphore(redis_client, "s3_guard", limit=1, lease_time=5.0)

        async def boom_start(self):
            raise RuntimeError("simulated guard start failure")

        monkeypatch.setattr(LeaseGuard, "start", boom_start)

        with pytest.raises(RuntimeError, match="guard start failure"):
            await sem.acquire(timeout=1.0)

        # pre-fix: ZSET residue, with limit=1 this would timeout
        monkeypatch.undo()
        token = await sem.acquire(timeout=0.5)
        await sem.release(token)

    async def test_in_use_failure_releases_slot(self, redis_client, monkeypatch):
        """in_use() 触网失败同样兜底归还（guard 登记前的失败也覆盖）。"""
        sem = AsyncLeaseSemaphore(redis_client, "s3_inuse", limit=1, lease_time=5.0)

        async def boom_in_use():
            raise RuntimeError("simulated in_use failure")

        monkeypatch.setattr(sem, "in_use", boom_in_use)

        with pytest.raises(RuntimeError, match="in_use failure"):
            await sem.acquire(timeout=1.0)

        monkeypatch.undo()
        token = await sem.acquire(timeout=0.5)
        await sem.release(token)


@pytest.mark.pinning
class TestS4DecodeNormalization:
    """S4：bytes client 全流程。"""

    async def test_bytes_client_acquire_release(self, redis_url):
        """decode_responses=False：acquire 不再死循环超时。"""
        import redis.asyncio as aioredis

        client = aioredis.from_url(redis_url, decode_responses=False)
        try:
            sem = AsyncLeaseSemaphore(client, "s4_bytes", limit=1, lease_time=5.0)
            # pre-fix: bytes != str always False → infinite loop until timeout
            token = await sem.acquire(timeout=1.0)
            assert token.value
            await sem.release(token)
        finally:
            await client.flushdb()
            await client.aclose()


@pytest.mark.pinning
class TestS5WatchdogStopLatency:
    """S5：stop 即时性。"""

    async def test_stop_returns_promptly(self, redis_client):
        """release（内含 guard.stop）近似即时返回。"""
        sem = AsyncLeaseSemaphore(
            redis_client, "s5_stop", limit=1, lease_time=30.0, renew_interval=10.0,
        )
        token = await sem.acquire(timeout=1.0)

        start = time.monotonic()
        await sem.release(token)
        elapsed = time.monotonic() - start

        # pre-fix: sleep(10) does not respond to stop_event → wait_for 1s timeout → cancel (~1s)
        # post-fix: stop_event wakes and exits (~0.01s); threshold 0.5s clearly distinguishes
        assert elapsed < 0.5, f"stop latency too high: {elapsed:.2f}s"

@pytest.mark.pinning
class TestB1UpdatePreservesTTL:
    """B1：update_task 不传 expiry 保持剩余到期时刻。"""

    async def test_update_without_expiry_preserves_ttl(self, redis_url, redis_client):
        """核心验收：不重置为 default 7 天。"""
        db = TaskRedisDB(redis_url, default_expire_time=604800)
        await db.connect()

        await db.initialize_task("b1_ttl", expiry=100)
        await db.update_task("b1_ttl", {"status": "in_progress"})  # 不传 expiry

        ttl = await redis_client.ttl("task:b1_ttl")
        # post-fix: ≈100; pre-fix: ≈604800
        assert 0 < ttl <= 100, f"TTL should be preserved (<=100), got {ttl}"
        await db.close()

    async def test_update_with_expiry_still_advances(self, redis_url, redis_client):
        """回归：显式传 expiry 仍推进租约（v0.3.2 test_update_refreshes_index_lease 同路径）。"""
        db = TaskRedisDB(redis_url, default_expire_time=604800)
        await db.connect()

        await db.initialize_task("b1_adv", expiry=100)
        await db.update_task("b1_adv", {"status": "in_progress"}, expiry=300)

        ttl = await redis_client.ttl("task:b1_adv")
        assert 250 < ttl <= 300, f"TTL should advance to ~300, got {ttl}"
        await db.close()

    async def test_index_lease_follows_preserved_ttl(self, redis_url, redis_client):
        """索引租约与主记录同口径：保持模式下按剩余 TTL 到期（成员级契约不破坏）。"""
        db = TaskRedisDB(redis_url, default_expire_time=604800)
        await db.connect()

        await db.initialize_task("b1_idx", expiry=4)
        await asyncio.sleep(1)  # 剩余 ~3s
        await db.update_task("b1_idx", {"status": "in_progress"})  # 保持剩余

        # index score should be now + ~3s (remaining), not now + 7 days
        server_time = await redis_client.time()
        now_ms = int(server_time[0]) * 1000 + int(server_time[1]) // 1000
        score = await redis_client.zscore("task_status:in_progress", "b1_idx")
        assert score is not None
        assert 0 < score - now_ms < 4000, (
            f"index lease should track preserved TTL, got +{score - now_ms}ms"
        )

        # after remaining TTL exceeds: main record and index disappear synchronously (no ghost, no extension)
        await asyncio.sleep(3.2)
        assert await db.get_task("b1_idx") == {}
        assert "b1_idx" not in await db.list_task_ids_by_status("in_progress")
        await db.close()


@pytest.mark.pinning
class TestB2QuotaIdempotentRenewal:
    """B2：quota 幂等命中刷新租约 score。"""

    async def test_already_reserved_refreshes_lease(self, redis_url):
        """重试同 task_id → score 刷新 → 崩溃回收不误清。

        修复前：score 停留首次时刻，2.2s 后被清理段当崩溃任务回收（pending=1）。
        修复后：score 刷新到重试时刻，1.2s 前 < 2s 窗口 → 保留（pending=6）。
        """
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        await db.reserve_units(
            scope="b2", task_id="t1", units=5, max_units=100, task_ttl_sec=2,
        )
        await asyncio.sleep(1.0)

        r = await db.reserve_units(
            scope="b2", task_id="t1", units=5, max_units=100, task_ttl_sec=2,
        )
        assert r["ok"] is True and r["reason"] == "already_reserved"

        await asyncio.sleep(1.2)  # 距首次 2.2s（>ttl），距重试 1.2s（<ttl）

        # use same ttl to trigger cleanup (cleanup window = task_ttl of this call)
        await db.reserve_units(
            scope="b2", task_id="t2", units=1, max_units=100, task_ttl_sec=2,
        )

        assert await db.get_pending_units(scope="b2") == 6, (
            "t1's lease should survive after idempotent retry refreshed its score"
        )
        await db.close()


@pytest.mark.pinning
class TestB3PoolHealthCheck:
    """B3：PoolManager 路径 health_check_interval（纯对象断言，无需 Redis 连接）。

    断言点说明：health_check_interval 由 pool 收进 connection_kwargs
    透传给每个 Connection——ConnectionPool 自身不挂同名属性。
    """

    def setup_method(self):
        get_pool_manager().clear()

    def test_default_health_check_interval(self):
        """默认补 30s（B9 纪律补齐）。"""
        manager = get_pool_manager()
        manager.register_pool("b3", "redis://localhost:6379/0")
        assert manager._pools["b3"].connection_kwargs["health_check_interval"] == 30

    def test_explicit_health_check_respected(self):
        """显式传参不被默认值覆盖（setdefault 语义）。"""
        manager = get_pool_manager()
        manager.register_pool(
            "b3x", "redis://localhost:6379/0", health_check_interval=60,
        )
        assert manager._pools["b3x"].connection_kwargs["health_check_interval"] == 60

@pytest.mark.pinning
class TestB4RegistrationDriftWarning:
    """B4：注册幂等的参数漂移 warning（幂等语义不变，回归断言同 v0.3.2）。"""

    def setup_method(self):
        get_registry().clear()
        get_pool_manager().clear()

    def test_semaphore_drift_warns(self, redis_client, caplog):
        registry = get_registry()
        sem1 = registry.register_semaphore("b4s", redis_client, limit=10, lease_time=5.0)

        with caplog.at_level(logging.WARNING):
            sem2 = registry.register_semaphore("b4s", redis_client, limit=99)

        assert sem2 is sem1 and sem2.limit == 10  # 首次参数生效（回归）
        assert any("different params" in r.message for r in caplog.records)

    def test_bucket_drift_warns(self, redis_client, caplog):
        registry = get_registry()
        registry.register_bucket("b4b", redis_client, 10, 1, 1.0)

        with caplog.at_level(logging.WARNING):
            bucket = registry.register_bucket("b4b", redis_client, 99, 1, 1.0)

        assert bucket.capacity == 10  # 首次参数生效（回归）
        assert any("different params" in r.message for r in caplog.records)

    def test_pool_drift_warns(self, caplog):
        manager = get_pool_manager()
        manager.register_pool("b4p", "redis://localhost:6379/0", max_connections=100)

        with caplog.at_level(logging.WARNING):
            manager.register_pool("b4p", "redis://localhost:6379/0", max_connections=999)

        assert manager._pools["b4p"].max_connections == 100  # 首次生效（回归）
        assert any("max_connections" in r.message for r in caplog.records)

    def test_same_params_no_warning(self, redis_client, caplog):
        """同参重复注册不告警（幂等正常路径不制造噪音）。"""
        registry = get_registry()
        registry.register_semaphore("b4_same", redis_client, limit=10, lease_time=5.0)

        with caplog.at_level(logging.WARNING):
            registry.register_semaphore("b4_same", redis_client, limit=10, lease_time=5.0)

        assert not any("different params" in r.message for r in caplog.records)