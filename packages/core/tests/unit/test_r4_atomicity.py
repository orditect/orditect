"""R4 修复验证：initialize_task 使用 pipeline 保证原子性。"""
import pytest

from orditect.core import TaskRedisDB, TaskStatus


class TestR4Atomicity:
    """验证 initialize_task 的原子性。"""

    async def test_initialize_task_atomic(self, redis_url, redis_client):
        """验证 initialize_task 后任务记录和状态索引同时存在。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        task_id = "test_r4_atomic"
        await db.initialize_task(task_id, initial_status=TaskStatus.pending.value)

        # verify task record exists
        task = await db.get_task(task_id)
        assert task["status"] == TaskStatus.pending.value

        # verify status index exists
        task_ids = await db.list_task_ids_by_status(TaskStatus.pending.value)
        assert task_id in task_ids

        await db.close()

    async def test_initialize_task_with_custom_status(self, redis_url, redis_client):
        """验证自定义初始状态的原子性。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        task_id = "test_r4_custom"
        await db.initialize_task(task_id, initial_status=TaskStatus.in_progress.value)

        # verify task record and status index consistent
        task = await db.get_task(task_id)
        assert task["status"] == TaskStatus.in_progress.value

        task_ids = await db.list_task_ids_by_status(TaskStatus.in_progress.value)
        assert task_id in task_ids

        # verify not in pending index
        pending_ids = await db.list_task_ids_by_status(TaskStatus.pending.value)
        assert task_id not in pending_ids

        await db.close()