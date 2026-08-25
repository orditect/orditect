"""LimiterRegistry 状态查询接口单元测试。

覆盖升级清单交付的 4 个查询方法：
- get_semaphore_limit(name)：同步，预设限制（纯内存读取）
- get_semaphore_usage(name)：async，实时使用量（近似值）
- get_semaphore_status(name)：async，单个信号量完整状态（五字段统一格式）
- get_all_semaphore_status()：async，所有信号量状态聚合

返回格式与 taskflow GovernorManager / taskstream StreamGovernorManager 对齐：
    {"name", "limit", "usage", "available", "utilization"}

注意：
- 涉及 Redis 读写的用例依赖 redis_client fixture（真实 Redis，不可用自动 skip）
- 未注册异常路径在字典直取处即抛 KeyError，不触网，无需 redis_client
- acquire 的用例必须 try/finally 释放，避免 LeaseGuard watchdog 协程泄漏
"""
import pytest

from orditect.core import get_registry


class TestGetSemaphoreLimit:
    """get_semaphore_limit() 测试（同步方法）。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        get_registry().clear()

    def test_get_limit(self, redis_client):
        """查询已注册信号量的预设限制。"""
        registry = get_registry()
        registry.register_semaphore("sem_limit", redis_client, limit=30, lease_time=5.0)

        assert registry.get_semaphore_limit("sem_limit") == 30

    def test_get_limit_not_registered(self):
        """未注册时抛 KeyError。"""
        registry = get_registry()
        with pytest.raises(KeyError):
            registry.get_semaphore_limit("not_registered")


class TestGetSemaphoreUsage:
    """get_semaphore_usage() 测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        get_registry().clear()

    async def test_usage_empty(self, redis_client):
        """未获取任何槽位时水位为 0。"""
        registry = get_registry()
        registry.register_semaphore("sem_usage", redis_client, limit=5, lease_time=5.0)

        assert await registry.get_semaphore_usage("sem_usage") == 0

    async def test_usage_reflects_acquire_release(self, redis_client):
        """acquire 后水位 +1，release 后归 0。"""
        registry = get_registry()
        sem = registry.register_semaphore("sem_usage", redis_client, limit=5, lease_time=5.0)

        token = await sem.acquire(timeout=1.0)
        try:
            assert await registry.get_semaphore_usage("sem_usage") == 1
        finally:
            await sem.release(token)

        assert await registry.get_semaphore_usage("sem_usage") == 0

    async def test_usage_not_registered(self):
        """未注册时抛 KeyError。"""
        registry = get_registry()
        with pytest.raises(KeyError):
            await registry.get_semaphore_usage("not_registered")


class TestGetSemaphoreStatus:
    """get_semaphore_status() 测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        get_registry().clear()

    async def test_status_fields(self, redis_client):
        """返回五字段统一格式（空水位基线）。"""
        registry = get_registry()
        registry.register_semaphore("sem_status", redis_client, limit=30, lease_time=5.0)

        status = await registry.get_semaphore_status("sem_status")

        assert status == {
            "name": "sem_status",
            "limit": 30,
            "usage": 0,
            "available": 30,
            "utilization": "0.0%",
        }

    async def test_status_utilization(self, redis_client):
        """limit=2 获取 1 个槽位后：usage=1 / available=1 / utilization='50.0%'。"""
        registry = get_registry()
        sem = registry.register_semaphore("sem_util", redis_client, limit=2, lease_time=5.0)

        token = await sem.acquire(timeout=1.0)
        try:
            status = await registry.get_semaphore_status("sem_util")
            assert status["name"] == "sem_util"
            assert status["limit"] == 2
            assert status["usage"] == 1
            assert status["available"] == 1
            assert status["utilization"] == "50.0%"
        finally:
            await sem.release(token)

    async def test_status_not_registered(self):
        """未注册时抛 KeyError。"""
        registry = get_registry()
        with pytest.raises(KeyError):
            await registry.get_semaphore_status("not_registered")


class TestGetAllSemaphoreStatus:
    """get_all_semaphore_status() 测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        get_registry().clear()

    async def test_empty_registry(self):
        """空注册表返回 {}。"""
        registry = get_registry()
        assert await registry.get_all_semaphore_status() == {}

    async def test_multiple_semaphores(self, redis_client):
        """聚合所有已注册信号量，键为资源名。"""
        registry = get_registry()
        registry.register_semaphore("sem_a", redis_client, limit=10, lease_time=5.0)
        registry.register_semaphore("sem_b", redis_client, limit=20, lease_time=5.0)

        all_status = await registry.get_all_semaphore_status()

        assert set(all_status.keys()) == {"sem_a", "sem_b"}
        assert all_status["sem_a"]["limit"] == 10
        assert all_status["sem_a"]["usage"] == 0
        assert all_status["sem_b"]["limit"] == 20
        assert all_status["sem_b"]["usage"] == 0

    async def test_bucket_not_included(self, redis_client):
        """令牌桶不出现在结果中（预约即消费，无水位概念）。"""
        registry = get_registry()
        registry.register_semaphore("sem_only", redis_client, limit=5, lease_time=5.0)
        registry.register_bucket("bucket_excluded", redis_client, 10, 1, 1.0)

        all_status = await registry.get_all_semaphore_status()

        assert set(all_status.keys()) == {"sem_only"}
        assert "bucket_excluded" not in all_status