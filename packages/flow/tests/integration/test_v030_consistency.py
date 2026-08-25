"""v0.3.0 集成验收：存储行为一致性 + R10 终态保护。

v0.3.0 批次 5 后：本地存储已删除，taskbase 升格硬依赖，
本文件聚焦：
- 1c 修复后的 cancel 行为在默认存储（taskbase）下的正确性
- R10 词汇表接线验收（succeeded 终态保护 + transitions 排雷）

需要真实 Redis（redis_client fixture，db15，不可用自动 skip）。

签名纪律：三框架 initialize_task 的第二位置参数语义不同
（taskflow 协议=initial_status，taskbase=expiry），
跨实现调用必须使用关键字参数，禁止位置参数。

时序纪律：协程启动后必须等 is_running 登记（确保过了 R5 检查、
进入正常执行路径），再触发 cancel——避免协程收尾跨越 fixture
关闭连接的事件循环边界（GeneratorExit warning 噪音）。
"""
import asyncio
import time

import pytest

from orditect.flow import (
    BaseBackEndTask,
    TaskExecutor,
    get_default_storage,
)

try:
    from orditect.core import InvalidStatusTransferError as TaskbaseInvalidTransfer
    HAS_TASKBASE = True
except ImportError:
    HAS_TASKBASE = False
    TaskbaseInvalidTransfer = None

requires_taskbase = pytest.mark.skipif(not HAS_TASKBASE, reason="orditect-core not installed")


class QuickTask(BaseBackEndTask):
    def __init__(self, storage, governor=None):
        super().__init__(storage, governor)
        self.hooks = []

    async def execute(self, task_id: str, **kwargs):
        await asyncio.sleep(0.1)
        return {"ok": True}

    async def on_success(self, task_id, result):
        self.hooks.append("on_success")

    async def on_cancel(self, task_id):
        self.hooks.append("on_cancel")


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.01):
    """轮询等待条件满足。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.mark.integration
@requires_taskbase
class TestStorageCancelConsistency:
    """1c 修复验收：cancel 行为在默认存储（taskbase）下的正确性。"""

    async def _run_cancel_scenario(self, storage):
        # keyword call: taskflow protocol differs from taskbase positional signature
        await storage.initialize_task(task_id="t_consist", initial_status="pending")
        executor = TaskExecutor(storage, governor=None)
        task = QuickTask(storage)

        runner = asyncio.create_task(executor.execute(task_id="t_consist", task=task))

        assert await _wait_for(lambda: executor.is_running("t_consist"))
        await asyncio.sleep(0.05)

        await executor.cancel("t_consist", force=False)
        await runner

        assert await _wait_for(lambda: not executor.is_running("t_consist"))

        # v0.3.3: drain shield cleanup (prevent coroutine surviving to later tests reported by GC)
        assert await _wait_for(lambda: len(executor._finalize_tasks) == 0, timeout=2.0)

        return await storage.get_task("t_consist"), task.hooks

    async def test_default_storage_cancel_behavior(self, redis_client):
        """默认存储（taskbase）：cancel 标记的任务跑完落 cancelled + on_cancel。"""
        storage = get_default_storage(redis_client)
        if hasattr(storage, "connect"):
            await storage.connect()

        stored, hooks = await self._run_cancel_scenario(storage)
        assert stored["status"] == "cancelled"
        assert hooks == ["on_cancel"]

        # v0.3.3: this test uses executor.execute (not orchestrator),
        # executor is local variable inside _run_cancel_scenario — needs drain after it returns.
        # but executor reference unavailable, waiting for global finalize drain not feasible —
        # simplest fix: drain inside helper (see helper modification below).


@pytest.mark.integration
@requires_taskbase
class TestR10TerminalProtection:
    """R10 接线验收：succeeded 后覆盖被 taskbase 终态保护拒绝。"""

    async def test_succeeded_cannot_be_overwritten(self, redis_client):
        storage = get_default_storage(redis_client)
        if hasattr(storage, "connect"):
            await storage.connect()

        await storage.initialize_task(task_id="t_r10", initial_status="pending")
        await storage.update_task("t_r10", {"status": "queued"}, validate_status_transfer=False)
        await storage.update_task("t_r10", {"status": "running"}, validate_status_transfer=False)
        await storage.update_task("t_r10", {"status": "succeeded"}, validate_status_transfer=False)

        # succeeded already terminal (taskflow vocabulary passed) → overwrite rejected
        with pytest.raises(TaskbaseInvalidTransfer):
            await storage.update_task(
                "t_r10", {"status": "failed"}, validate_status_transfer=False
            )

        stored = await storage.get_task("t_r10")
        assert stored["status"] == "succeeded"

    async def test_transitions_table_no_landmine(self, redis_client):
        """transitions 排雷验收：validate=True 路径不被 taskbase 默认表误杀。"""
        storage = get_default_storage(redis_client)
        if hasattr(storage, "connect"):
            await storage.connect()

        await storage.initialize_task(task_id="t_trans", initial_status="pending")
        # validate=True: taskflow vocabulary allows pending→queued (taskbase default table lacks this transition)
        await storage.update_task(
            "t_trans", {"status": "queued"}, validate_status_transfer=True
        )
        stored = await storage.get_task("t_trans")
        assert stored["status"] == "queued"