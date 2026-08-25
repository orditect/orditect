"""配额管理集成测试。"""
import pytest

from orditect.core import AdmissionQuotaRedisDB


@pytest.mark.integration
class TestQuotaDB:
    """配额预占/释放测试。"""

    async def test_reserve_and_release(self, redis_url):
        """预占 + 释放。"""
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        # pre-occupy
        result = await db.reserve_units(
            scope="test",
            task_id="task_001",
            units=10,
            max_units=50,
        )
        assert result["ok"] is True
        assert result["current"] == 10

        # query current usage
        pending = await db.get_pending_units(scope="test")
        assert pending == 10

        # release
        result = await db.release_units(scope="test", task_id="task_001")
        assert result["ok"] is True
        assert result["released"] == 10

        # usage returns to zero after release
        pending = await db.get_pending_units(scope="test")
        assert pending == 0

        await db.close()

    async def test_reserve_exceeds_limit(self, redis_url):
        """超限预占被拒绝。"""
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        result = await db.reserve_units(
            scope="test_limit",
            task_id="task_002",
            units=100,
            max_units=50,
        )
        assert result["ok"] is False
        assert result["reason"] == "limit_exceeded"

        await db.close()