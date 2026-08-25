"""GovernedClient 单元测试（升级清单任务 2：双层治理之全局调用点治理）。

覆盖：
- 调用前 acquire、调用后 finally release（含异常路径）
- handler 构造时绑定 / call 时覆盖
- cancel_token：acquire 前取消（不 acquire 直接跳过）、
  acquire 后取消（跳过执行但正常 release）
- gather 批量场景的部分取消（返回 None 不炸 gather）
- 构造参数校验

测试基建为纯内存 FakeGovernor / FakeCancelToken，无需 Redis。
"""
import asyncio
from typing import Any, List, Optional

import pytest

from orditect.flow import GovernedClient


# ---------- test infrastructure ----------

class FakeGovernor:
    """内存治理：记录 acquire/release 调用，可观察令牌生命周期。"""

    def __init__(self):
        self.acquired: List[str] = []
        self.released: List[str] = []
        self.last_timeout: Optional[float] = None
        self._counter = 0

    async def acquire(self, resource: str, timeout: Optional[float] = None) -> str:
        self.acquired.append(resource)
        self.last_timeout = timeout
        self._counter += 1
        return f"fake-token-{self._counter}"

    async def try_acquire(self, resource: str) -> Optional[str]:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


class FakeCancelToken:
    """可切换状态的取消令牌（鸭子类型：async is_cancelled）。"""

    def __init__(self, cancelled: bool = False):
        self._cancelled = cancelled

    async def is_cancelled(self) -> bool:
        return self._cancelled


class FlipCancelToken:
    """第一次 is_cancelled 返回 False，之后返回 True。

    用于命中"acquire 后发现已取消"路径：
    第 1 次检查（acquire 前）→ False，第 2 次检查（acquire 后）→ True。
    """

    def __init__(self):
        self.calls = 0

    async def is_cancelled(self) -> bool:
        self.calls += 1
        return self.calls > 1


# ---------- test cases ----------

class TestGovernedClientBasics:
    """基础调用与令牌生命周期。"""

    async def test_call_executes_handler_and_releases(self):
        """正常调用：acquire → 执行 → release，位置/关键字参数透传。"""
        governor = FakeGovernor()

        async def add(a, b, scale=1):
            return (a + b) * scale

        client = GovernedClient(governor, "res_a", handler=add)
        result = await client.call(1, 2, scale=10)

        assert result == 30
        assert governor.acquired == ["res_a"]
        assert governor.released == ["res_a"]

    async def test_handler_override_at_call_time(self):
        """call 时传入 handler 覆盖构造时绑定。"""
        governor = FakeGovernor()

        async def handler_a():
            return "a"

        async def handler_b():
            return "b"

        client = GovernedClient(governor, "res_a", handler=handler_a)
        result = await client.call(handler=handler_b)

        assert result == "b"

    async def test_handler_can_be_omitted_at_construction(self):
        """构造时不绑 handler，call 时传入。"""
        governor = FakeGovernor()

        async def work(x):
            return x * 2

        client = GovernedClient(governor, "res_a")
        assert await client.call(21, handler=work) == 42

    async def test_missing_handler_raises(self):
        """构造与调用均未提供 handler 时抛 ValueError。"""
        governor = FakeGovernor()
        client = GovernedClient(governor, "res_a")

        with pytest.raises(ValueError, match="handler"):
            await client.call()

        # no token acquired
        assert governor.acquired == []

    async def test_exception_still_releases(self):
        """handler 抛异常时令牌仍在 finally 释放。"""
        governor = FakeGovernor()

        async def boom():
            raise ValueError("handler failed")

        client = GovernedClient(governor, "res_a", handler=boom)

        with pytest.raises(ValueError, match="handler failed"):
            await client.call()

        assert governor.acquired == ["res_a"]
        assert governor.released == ["res_a"]

    async def test_custom_acquire_timeout(self):
        """自定义 acquire 超时透传给 governor。"""
        governor = FakeGovernor()

        async def work():
            return "ok"

        client = GovernedClient(governor, "res_a", handler=work, timeout=5.0)
        await client.call()

        assert governor.last_timeout == 5.0


class TestGovernedClientValidation:
    """构造参数校验。"""

    def test_none_governor_rejected(self):
        with pytest.raises(ValueError, match="governor"):
            GovernedClient(None, "res_a")

    def test_empty_resource_rejected(self):
        with pytest.raises(ValueError, match="resource"):
            GovernedClient(FakeGovernor(), "")


class TestGovernedClientCancel:
    """cancel_token 取消语义。"""

    async def test_cancelled_before_acquire_skips_without_acquire(self):
        """调用前已取消：不 acquire、不执行、返回 None。"""
        governor = FakeGovernor()
        executed = []

        async def work():
            executed.append(True)
            return "ok"

        client = GovernedClient(governor, "res_a", handler=work)
        token = FakeCancelToken(cancelled=True)

        result = await client.call(cancel_token=token)

        assert result is None
        assert executed == []  # handler 未执行
        assert governor.acquired == []  # 未获取令牌（不浪费排队名额）
        assert governor.released == []

    async def test_cancelled_after_acquire_releases_token(self):
        """acquire 后发现已取消：不执行、返回 None，令牌在 finally 正常释放。"""
        governor = FakeGovernor()
        executed = []

        async def work():
            executed.append(True)
            return "ok"

        client = GovernedClient(governor, "res_a", handler=work)
        token = FlipCancelToken()  # acquire 前 False，acquire 后 True

        result = await client.call(cancel_token=token)

        assert result is None
        assert executed == []  # handler 未执行
        assert governor.acquired == ["res_a"]  # 已获取
        assert governor.released == ["res_a"]  # 已释放（杜绝泄漏）

    async def test_not_cancelled_executes_normally(self):
        """未取消时正常执行。"""
        governor = FakeGovernor()

        async def work():
            return "ok"

        client = GovernedClient(governor, "res_a", handler=work)
        result = await client.call(cancel_token=FakeCancelToken(cancelled=False))

        assert result == "ok"
        assert governor.released == ["res_a"]

    async def test_gather_partial_cancel(self):
        """gather 批量场景：部分取消返回 None，不影响其他调用。"""
        governor = FakeGovernor()

        async def work(x):
            return x * 2

        client = GovernedClient(governor, "res_a", handler=work)
        cancelled = FakeCancelToken(cancelled=True)
        alive = FakeCancelToken(cancelled=False)

        results = await asyncio.gather(
            client.call(1, cancel_token=cancelled),
            client.call(2, cancel_token=alive),
        )

        assert results == [None, 4]