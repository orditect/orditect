"""RedisPoolManager 单元测试。"""
import pytest

from orditect.core import get_pool_manager, RedisPoolManager


class TestRedisPoolManager:
    """连接池管理器测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        manager = get_pool_manager()
        manager.clear()

    def test_register_pool(self):
        """注册连接池。"""
        manager = get_pool_manager()
        client = manager.register_pool(
            "test",
            redis_url="redis://localhost:6379/0",
            max_connections=100,
        )

        assert client is not None
        assert manager.has_pool("test")

    def test_register_pool_idempotent(self):
        """重复注册返回同一实例（幂等）。"""
        manager = get_pool_manager()
        client1 = manager.register_pool("test", "redis://localhost:6379/0")
        client2 = manager.register_pool("test", "redis://localhost:6379/0", max_connections=200)

        assert client1 is client2  # 同一实例

    def test_get_client(self):
        """获取已注册的客户端。"""
        manager = get_pool_manager()
        manager.register_pool("test", "redis://localhost:6379/0")

        client = manager.get_client("test")
        assert client is not None

    def test_get_client_not_registered(self):
        """获取未注册的客户端应抛 KeyError。"""
        manager = get_pool_manager()
        with pytest.raises(KeyError):
            manager.get_client("not_registered")

    async def test_get_pool_stats(self):
        """获取连接池统计信息。"""
        manager = get_pool_manager()
        manager.register_pool("test", "redis://localhost:6379/0", max_connections=100)

        stats = await manager.get_pool_stats("test")
        assert stats["name"] == "test"
        assert stats["max_connections"] == 100
        assert "in_use" in stats
        assert "available" in stats
        assert "utilization" in stats

    def test_clear(self):
        """清空注册表。"""
        manager = get_pool_manager()
        manager.register_pool("test", "redis://localhost:6379/0")

        manager.clear()

        assert not manager.has_pool("test")


class TestRedisDBDependencyInjection:
    """RedisDB 依赖注入测试。"""

    async def test_redis_db_with_client(self, redis_client):
        """依赖注入模式：RedisDB(client=...)。"""
        from orditect.core import RedisDB, InvalidUsageError

        db = RedisDB(client=redis_client)
        assert db.client is redis_client
        assert db._owns_pool is False

        # connect() should be no-op
        await db.connect()

        # v0.3.1: DI mode close() raises InvalidUsageError (pool lifecycle belongs to PoolManager)
        with pytest.raises(InvalidUsageError):
            await db.close()

    async def test_redis_db_with_url(self, redis_url):
        """自管理模式：RedisDB(redis_url=...)。"""
        from orditect.core import RedisDB

        db = RedisDB(redis_url=redis_url)
        assert db._owns_pool is True

        await db.connect()
        assert db.client is not None

        await db.close()

    async def test_redis_db_missing_args(self):
        """缺少 redis_url 和 client 应抛 ValueError。"""
        from orditect.core import RedisDB

        with pytest.raises(ValueError, match="Must provide redis_url or client"):
            RedisDB()