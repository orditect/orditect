"""P1 行为钉扎测试：TaskRedisDB（钉住搬运前行为，含已知缺陷 R7）。

这些测试在 P2 手术时会被**翻转断言**：
- test_r7_terminal_protection_bypassed_when_validate_false：P1 钉住 bug，P2 修复后改为拒绝
"""
import pytest

from orditect.core import TaskRedisDB, TaskStatus, InvalidStatusTransferError


@pytest.mark.pinning
class TestTaskDBBasics:
    """TaskRedisDB 基础行为钉扎。"""

    async def test_initialize_and_get(self, redis_client, redis_url):
        """初始化 + 读取任务。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("task_001")
        task = await db.get_task("task_001")

        assert task["status"] == "pending"
        assert task["cancel_requested"] is False
        assert "timestamp" in task

        await db.close()

    async def test_update_task_status(self, redis_client, redis_url):
        """更新任务状态（合法流转）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("task_002")
        await db.update_task("task_002", {"status": "in_progress"})

        task = await db.get_task("task_002")
        assert task["status"] == "in_progress"

        await db.close()

    async def test_invalid_status_transfer_rejected(self, redis_client, redis_url):
        """非法流转被拒绝（Python 侧校验）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("task_003")

        with pytest.raises(InvalidStatusTransferError):
            await db.update_task("task_003", {"status": "completed"})  # pending -> completed 不允许

        await db.close()


@pytest.mark.pinning
class TestR7TerminalProtection:
    """R7 修复钉扎：validate=False 时 Lua 终态保护无条件生效。"""

    async def test_r7_terminal_protection_enforced_when_validate_false(self, redis_client, redis_url):
        """P2 修复后：validate=False 时终态保护仍然生效（Lua 侧无条件保护）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        # normal transition to completed
        await db.initialize_task("task_r7")
        await db.update_task("task_r7", {"status": "in_progress"})
        await db.update_task("task_r7", {"status": "completed"})

        # after P2 fix: Lua terminal protection still effective even with validate=False, refuses overwrite
        with pytest.raises(InvalidStatusTransferError, match="invalid status transfer"):
            await db.update_task(
                "task_r7",
                {"status": "failed"},
                validate_status_transfer=False,  # 旁路 Python 校验，但 Lua 保护生效
            )

        # verify status not overwritten
        task = await db.get_task("task_r7")
        assert task["status"] == "completed"  # 仍然是 completed

        await db.close()

@pytest.mark.pinning
class TestCancelFlow:
    """取消流程钉扎。"""

    async def test_request_cancel(self, redis_client, redis_url):
        """请求取消任务。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("task_cancel")
        ok = await db.request_cancel("task_cancel")

        assert ok is True

        task = await db.get_task("task_cancel")
        assert task["cancel_requested"] is True

        await db.close()