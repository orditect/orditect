"""ResourceGovernorProtocol 协议测试。"""
import pytest

from orditect.stream.protocols.governor import ResourceGovernorProtocol


class MockGovernor:
    """Mock governor 实现。"""

    def __init__(self):
        self.tokens: dict[str, str] = {}
        self.usage: dict[str, int] = {}

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        token = f"token_{resource}_{len(self.tokens)}"
        self.tokens[token] = resource
        self.usage[resource] = self.usage.get(resource, 0) + 1
        return token

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        if token in self.tokens:
            del self.tokens[token]
            self.usage[resource] = max(0, self.usage.get(resource, 0) - 1)

    async def get_usage(self, resource: str) -> int:
        return self.usage.get(resource, 0)


class TestResourceGovernorProtocol:
    async def test_mock_governor_satisfies_protocol(self):
        """Mock governor 满足协议（鸭子类型）。"""
        governor: ResourceGovernorProtocol = MockGovernor()

        token = await governor.acquire("test_resource")
        assert token is not None

        usage = await governor.get_usage("test_resource")
        assert usage == 1

        await governor.release("test_resource", token)
        usage = await governor.get_usage("test_resource")
        assert usage == 0

    async def test_try_acquire(self):
        governor = MockGovernor()
        token = await governor.try_acquire("test")
        assert token is not None