"""R3 收尾验证：reconnect() 已物理删除（v0.3.1）。"""
import pytest

from orditect.core import RedisDB, InvalidUsageError


class TestR3Removal:
    """验证 reconnect() 不存在，DI 模式 close() 抛 InvalidUsageError。"""

    def test_reconnect_removed(self, redis_url):
        """reconnect 方法已从 API 中物理删除。"""
        db = RedisDB(redis_url)
        assert not hasattr(db, "reconnect"), "reconnect() should be removed in v0.3.1"

    async def test_close_di_mode_raises(self, redis_client):
        """DI 模式调用 close() 抛 InvalidUsageError（契约显式化）。"""
        db = RedisDB(client=redis_client)
        with pytest.raises(InvalidUsageError, match="dependency-injected"):
            await db.close()

    async def test_close_self_managed_works(self, redis_url):
        """自管理模式 close() 正常工作（回归）。"""
        db = RedisDB(redis_url)
        await db.connect()
        await db.close()  # 不抛异常即通过