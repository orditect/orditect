"""资源注册表单元测试。"""
import pytest

from orditect.core import get_registry


class TestLimiterRegistry:
    """资源注册表测试。"""

    def setup_method(self):
        """每个测试前清空注册表。"""
        registry = get_registry()
        registry.clear()

    def test_register_semaphore(self, redis_client):
        """注册信号量。"""
        registry = get_registry()
        sem = registry.register_semaphore(
            "test_sem",
            client=redis_client,
            limit=5,
            lease_time=30.0,
        )

        assert sem.name == "test_sem"
        assert sem.limit == 5
        assert registry.has_semaphore("test_sem")

    def test_register_semaphore_idempotent(self, redis_client):
        """重复注册返回同一实例（幂等）。"""
        registry = get_registry()
        sem1 = registry.register_semaphore("test_sem", redis_client, limit=5)
        sem2 = registry.register_semaphore("test_sem", redis_client, limit=10)

        assert sem1 is sem2  # 同一实例
        assert sem1.limit == 5  # 第一次注册的参数生效

    def test_get_semaphore(self, redis_client):
        """获取已注册的信号量。"""
        registry = get_registry()
        registry.register_semaphore("test_sem", redis_client, limit=5)

        sem = registry.get_semaphore("test_sem")
        assert sem.name == "test_sem"

    def test_get_semaphore_not_registered(self):
        """获取未注册的信号量应抛 KeyError。"""
        registry = get_registry()
        with pytest.raises(KeyError):
            registry.get_semaphore("not_registered")

    def test_register_bucket(self, redis_client):
        """注册令牌桶。"""
        registry = get_registry()
        bucket = registry.register_bucket(
            "test_bucket",
            client=redis_client,
            capacity=10,
            refill_amount=1,
            refill_frequency=1.0,
        )

        assert bucket.name == "test_bucket"
        assert bucket.capacity == 10
        assert registry.has_bucket("test_bucket")

    def test_get_bucket(self, redis_client):
        """获取已注册的令牌桶。"""
        registry = get_registry()
        registry.register_bucket("test_bucket", redis_client, 10, 1, 1.0)

        bucket = registry.get_bucket("test_bucket")
        assert bucket.name == "test_bucket"

    def test_clear(self, redis_client):
        """清空注册表。"""
        registry = get_registry()
        registry.register_semaphore("sem1", redis_client, limit=5)
        registry.register_bucket("bucket1", redis_client, 10, 1, 1.0)

        registry.clear()

        assert not registry.has_semaphore("sem1")
        assert not registry.has_bucket("bucket1")