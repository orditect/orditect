"""v0.3.0 钉扎测试：TaskRedisDB 宿主化（T1/T4/T6/T7）。

钉扎纪律：每个用例在修复前应失败（或能力不存在），修复后转绿。
"""
import pytest
import asyncio
from orditect.core import TaskRedisDB, TaskStatus, InvalidStatusTransferError


@pytest.mark.pinning
class TestT1CustomTerminalStatuses:
    """T1：终态集合实例级注入（方案 B 验收）。"""

    async def test_declared_terminal_protected(self, redis_url, redis_client):
        """声明的终态词（succeeded）享受 Lua 终态保护——修复前可被静默覆盖（R10）。"""
        db = TaskRedisDB(
            redis_url,
            terminal_statuses=("succeeded", "failed", "cancelled"),
            transitions={
                "": {"pending", "queued"},
                "pending": {"queued", "cancelled"},
                "queued": {"running", "cancelled"},
                "running": {"succeeded", "failed", "cancelled"},
                "succeeded": set(),
                "failed": set(),
                "cancelled": set(),
            },
        )
        await db.connect()

        await db.initialize_task("t1_succeeded", initial_status="pending")
        # move to succeeded along custom transition table
        await db.update_task("t1_succeeded", {"status": "queued"})
        await db.update_task("t1_succeeded", {"status": "running"})
        await db.update_task("t1_succeeded", {"status": "succeeded"})

        # succeeded is declared terminal: validate=False bypasses Python validation, Lua still should reject
        with pytest.raises(InvalidStatusTransferError):
            await db.update_task(
                "t1_succeeded",
                {"status": "failed"},
                validate_status_transfer=False,
            )

        # status not overwritten
        task = await db.get_task("t1_succeeded")
        assert task["status"] == "succeeded"
        await db.close()

    async def test_undeclared_word_not_protected(self, redis_url, redis_client):
        """未声明的词（done）不享受终态保护（白名单语义，防过度保护）。"""
        db = TaskRedisDB(
            redis_url,
            terminal_statuses=("succeeded", "failed", "cancelled"),
        )
        await db.connect()

        # done not in terminal set, no transition table constraint (validate=False direct write)
        await db.initialize_task("t1_done", initial_status="pending")
        await db.update_task("t1_done", {"status": "done"}, validate_status_transfer=False)

        # done is not declared terminal: allowed to change (whitelist only protects declared words)
        await db.update_task("t1_done", {"status": "running"}, validate_status_transfer=False)
        task = await db.get_task("t1_done")
        assert task["status"] == "running"
        await db.close()

    async def test_default_vocabulary_unchanged(self, redis_url, redis_client):
        """默认参数下词汇表与行为与 v0.2 完全一致（回归网）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t1_default")
        await db.update_task("t1_default", {"status": "in_progress"})
        await db.update_task("t1_default", {"status": "completed"})

        with pytest.raises(InvalidStatusTransferError):
            await db.update_task(
                "t1_default", {"status": "failed"}, validate_status_transfer=False
            )
        await db.close()


@pytest.mark.pinning
class TestT4IdempotentInitialize:
    """T4：初始化幂等（B3）。"""

    async def test_if_not_exists_skips_running_task(self, redis_url, redis_client):
        """running 任务重入 if_not_exists 初始化，状态不被抹回初始。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_idem")
        await db.update_task("t_idem", {"status": "in_progress"})

        # reinitialize (simulate parent task retry resubmit child task)
        ok = await db.initialize_task("t_idem", if_not_exists=True)
        assert ok is False

        task = await db.get_task("t_idem")
        assert task["status"] == "in_progress"  # 未被抹回 pending
        await db.close()

    async def test_if_not_exists_false_keeps_legacy_behavior(self, redis_url, redis_client):
        """默认（if_not_exists=False）保持旧行为：重复初始化覆盖记录。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_legacy")
        await db.update_task("t_legacy", {"status": "in_progress"})

        ok = await db.initialize_task("t_legacy")
        assert ok is True
        task = await db.get_task("t_legacy")
        assert task["status"] == "pending"  # 旧行为：覆盖
        await db.close()


@pytest.mark.pinning
class TestT6LineageIndex:
    """T6：谱系索引（B2）。"""

    async def test_parent_registration_and_list_children(self, redis_url, redis_client):
        """parent_task_id 写入记录 + children 集合可查询。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("parent")
        await db.initialize_task("child_1", parent_task_id="parent")
        await db.initialize_task("child_2", parent_task_id="parent")
        await db.initialize_task("orphan")  # 无父任务，不进索引

        children = await db.list_children("parent")
        assert set(children) == {"child_1", "child_2"}

        # record carries lineage fields
        child = await db.get_task("child_1")
        assert child["parent_task_id"] == "parent"

        # parent task without children returns empty list
        assert await db.list_children("orphan") == []
        await db.close()


@pytest.mark.pinning
class TestT7ExplicitExpiry:
    """T7：显式 expiry 不再被默认值吞掉（R5）。"""

    async def test_explicit_expiry_respected(self, redis_url, redis_client):
        """显式传入的 expiry 生效（而非 default_expire_time）。"""
        db = TaskRedisDB(redis_url, default_expire_time=604800)
        await db.connect()

        await db.initialize_task("t_expiry", expiry=3600)
        ttl = await redis_client.ttl("task:t_expiry")
        assert 3500 < ttl <= 3600  # 显式 3600 生效，不是 604800
        await db.close()


@pytest.mark.pinning
class TestT9IndexLifecycle:
    """T9：
    断言兼容，语义更新为"成员级租约清理"
    """

    async def test_status_index_expires_with_task(self, redis_url, redis_client):
        """主记录过期时，状态索引同步消失（无幽灵成员积累）。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("t_co_expire", expiry=1)
        assert "t_co_expire" in await db.list_task_ids_by_status("pending")

        await asyncio.sleep(1.5)

        # main record and index expire synchronously
        assert await db.get_task("t_co_expire") == {}
        assert "t_co_expire" not in await db.list_task_ids_by_status("pending")
        await db.close()

    async def test_children_index_expires_with_task(self, redis_url, redis_client):
        """谱系索引随子任务初始化时的 TTL 过期。"""
        db = TaskRedisDB(redis_url)
        await db.connect()

        await db.initialize_task("parent_co", expiry=100)
        await db.initialize_task("child_co", expiry=1, parent_task_id="parent_co")

        assert await db.list_children("parent_co") == ["child_co"]

        await asyncio.sleep(1.5)

        # lineage index (TTL=child init expiry) expires synchronously
        assert await db.list_children("parent_co") == []
        # parent main record still exists (TTL=100)
        parent = await db.get_task("parent_co")
        assert parent != {}
        assert parent["status"] == "pending"
        await db.close()