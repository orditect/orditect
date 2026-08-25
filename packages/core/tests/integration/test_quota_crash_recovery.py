"""P2 修复验证：任务崩溃后配额自动回收。"""
import asyncio

import pytest

from orditect.core import AdmissionQuotaRedisDB


@pytest.mark.integration
class TestQuotaCrashRecovery:
    """验证任务崩溃后配额自动回收。"""

    async def test_crashed_task_quota_recycled(self, redis_url, redis_client):
        """任务崩溃（未 release），配额应在 TTL 后自动回收。"""
        db = AdmissionQuotaRedisDB(redis_url)
        await db.connect()

        scope = "test_crash"
        task_id = "crashed_task"
        units = 10
        ttl = 2  # 2 秒 TTL

        # pre-occupy quota
        result = await db.reserve_units(
            scope=scope,
            task_id=task_id,
            units=units,
            max_units=50,
            task_ttl_sec=ttl,
        )
        assert result["ok"] is True
        assert result["current"] == 10

        # simulate task crash (no release_units)

        # wait TTL expiry
        await asyncio.sleep(ttl + 1)

        # pre-occupy again (triggers cleanup logic)
        result2 = await db.reserve_units(
            scope=scope,
            task_id="new_task",
            units=5,
            max_units=50,
            task_ttl_sec=ttl,
        )

        # after P2 fix: crashed task quota should be reclaimed, current should be 5 (not 15)
        assert result2["ok"] is True
        assert result2["current"] == 5  # 崩溃任务的 10 被回收

        await db.close()