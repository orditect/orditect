"""GovernorManager 单元测试（升级清单任务 4：面向业务的资源状态查询）。

v0.3.0：DefaultResourceGovernor 已删除（taskbase 升格硬依赖），
降级路径测试改用文件内 FakeGovernor（带 get_limit/list_resources
鸭子类型扩展）——本文件测的是 GovernorManager 的查询逻辑，
不是某个具体 governor 实现。

覆盖：
- taskbase registry 优先路径：已注册资源经 registry 查询，
  返回格式统一（"resource" key，内部从 taskbase "name" 转换）
- 降级路径：registry 未注册的资源经 FakeGovernor 查询
- 单资源不存在：ValueError
- get_all_resources：registry + governor 来源合并，governor 不覆盖 registry

taskbase 相关用例：orditect.core 未安装时自动 skip（松散耦合，
taskflow 不强依赖 taskbase）。taskbase 安装时，registry 路径测试
需要真实 Redis（semaphore.in_use() 触网），不可用时自动 skip。
"""
import pytest

from orditect.flow import (
    GovernorManager,
    UnlimitedGovernor,
)

# taskbase optional dependency detection (loose coupling: skip registry path tests if not installed)
try:
    import orditect.core  # noqa: F401
    from orditect.core import get_registry

    HAS_TASKBASE = True
except ImportError:
    HAS_TASKBASE = False
    get_registry = None

requires_taskbase = pytest.mark.skipif(
    not HAS_TASKBASE, reason="orditect-core not installed"
)


# ---------- test infrastructure ----------

class FakeGovernor:
    """内存治理（带 get_limit/list_resources 鸭子类型扩展，供降级路径测试）。

    模拟原 DefaultResourceGovernor 的对外接口面：
    协议方法（acquire/try_acquire/release/get_usage）
    + 状态查询扩展（get_limit/list_resources）。
    """

    def __init__(self, default_limit: int = 10):
        self.default_limit = default_limit
        self._limits: dict[str, int] = {}
        self._usage: dict[str, int] = {}
        self._counter = 0

    def set_limit(self, resource: str, limit: int) -> None:
        self._limits[resource] = limit

    async def acquire(self, resource: str, timeout=None) -> str:
        self._counter += 1
        self._usage[resource] = self._usage.get(resource, 0) + 1
        return f"fake-token-{self._counter}"

    async def try_acquire(self, resource: str):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self._usage[resource] = max(0, self._usage.get(resource, 0) - 1)

    async def get_usage(self, resource: str) -> int:
        return self._usage.get(resource, 0)

    def get_limit(self, resource: str) -> int:
        """Sync implementation: pins the v0.1.6 sync/async tolerance in
        GovernorManager.get_resource_status (previously a bare await raised
        TypeError on sync get_limit implementations)."""
        return self._limits.get(resource, self.default_limit)

    def list_resources(self) -> list[str]:
        """同步实现：验证 isawaitable 兼容路径。"""
        return list(self._limits)


# ---------- degraded path (pure in-memory, no taskbase/Redis needed) ----------

class TestGovernorManagerFallback:
    """降级路径：registry 未命中时直接查询 governor。"""

    async def test_get_resource_status_from_governor(self):
        """FakeGovernor：显式 set_limit 的资源可查询。"""
        governor = FakeGovernor(default_limit=10)
        governor.set_limit("task_agent", 5)

        manager = GovernorManager(governor)
        status = await manager.get_resource_status("task_agent")

        assert status == {
            "resource": "task_agent",
            "limit": 5,
            "usage": 0,
            "available": 5,
            "utilization": "0.0%",
        }

    async def test_fallback_usage_reflects_acquire(self):
        """降级路径 usage 反映实际获取量。"""
        governor = FakeGovernor(default_limit=10)
        governor.set_limit("res_a", 2)

        token = await governor.acquire("res_a")
        try:
            manager = GovernorManager(governor)
            status = await manager.get_resource_status("res_a")
            assert status["usage"] == 1
            assert status["available"] == 1
            assert status["utilization"] == "50.0%"
        finally:
            await governor.release("res_a", token)

    async def test_fallback_unknown_resource_uses_default_limit(self):
        """未 set_limit 的资源：get_limit 回退 default_limit。

        前提：本测试运行环境未安装 taskbase（或该资源未在 registry 注册），
        否则 registry 未命中后才会走到 governor 的 default_limit 回退。
        为保证确定性，仅在无 taskbase 时运行本用例。
        """
        if HAS_TASKBASE:
            pytest.skip("taskbase installed: default_limit fallback path is ambiguous")
        governor = FakeGovernor(default_limit=7)
        manager = GovernorManager(governor)

        status = await manager.get_resource_status("never_seen")
        assert status["limit"] == 7
        assert status["usage"] == 0

    async def test_resource_not_found_raises(self):
        """registry 与 governor 均无此资源：ValueError。"""
        manager = GovernorManager(UnlimitedGovernor())  # 无 get_limit

        with pytest.raises(ValueError, match="Resource not found"):
            await manager.get_resource_status("ghost")

    async def test_get_all_resources_empty(self):
        """无已知资源时返回 {}。"""
        manager = GovernorManager(UnlimitedGovernor())
        assert await manager.get_all_resources() == {}

    async def test_get_all_resources_from_governor(self):
        """降级聚合：governor.list_resources() 枚举的资源全部返回。"""
        if HAS_TASKBASE:
            pytest.skip("taskbase installed: governor-only aggregation is ambiguous")
        governor = FakeGovernor(default_limit=10)
        governor.set_limit("res_a", 3)
        governor.set_limit("res_b", 5)

        manager = GovernorManager(governor)
        all_status = await manager.get_all_resources()

        assert set(all_status.keys()) == {"res_a", "res_b"}
        assert all_status["res_a"]["limit"] == 3
        assert all_status["res_a"]["resource"] == "res_a"
        assert all_status["res_b"]["limit"] == 5


# ---------- taskbase registry priority path ----------

@requires_taskbase
class TestGovernorManagerTaskbase:
    """registry 优先路径（需要 orditect-core + 真实 Redis）。

    semaphore.in_use() 需访问 Redis 清理过期 token，
    因此本组测试复用 conftest 的 redis_client fixture（db15，跑前清场）。
    """

    def setup_method(self):
        """每个测试前清空 taskbase 全局注册表。"""
        get_registry().clear()

    def teardown_method(self):
        """每个测试后清空，避免污染其他测试。"""
        get_registry().clear()

    async def test_registry_takes_priority(self, redis_client):
        """registry 已注册的资源：经 taskbase 查询接口返回（"resource" key 转换）。"""
        registry = get_registry()
        registry.register_semaphore("llm", redis_client, limit=30, lease_time=5.0)

        # same resource in governor has different limit → should return registry's 30, proves priority
        governor = FakeGovernor(default_limit=10)
        governor.set_limit("llm", 999)

        manager = GovernorManager(governor)
        status = await manager.get_resource_status("llm")

        assert status == {
            "resource": "llm",
            "limit": 30,
            "usage": 0,
            "available": 30,
            "utilization": "0.0%",
        }

    async def test_registry_usage_reflects_acquire(self, redis_client):
        """registry 路径 usage 反映实际获取量。"""
        registry = get_registry()
        sem = registry.register_semaphore("llm", redis_client, limit=2, lease_time=5.0)

        token = await sem.acquire(timeout=1.0)
        try:
            manager = GovernorManager(governor=None)
            status = await manager.get_resource_status("llm")
            assert status["usage"] == 1
            assert status["available"] == 1
            assert status["utilization"] == "50.0%"
        finally:
            await sem.release(token)

    async def test_unregistered_falls_back_to_governor(self, redis_client):
        """registry 未注册的资源：降级到 governor 查询。"""
        governor = FakeGovernor(default_limit=10)
        governor.set_limit("local_only", 4)

        manager = GovernorManager(governor)
        status = await manager.get_resource_status("local_only")

        assert status["resource"] == "local_only"
        assert status["limit"] == 4

    async def test_get_all_resources_merges_sources(self, redis_client):
        """聚合：registry 为主 + governor 补充，governor 不覆盖 registry。"""
        registry = get_registry()
        registry.register_semaphore("shared", redis_client, limit=30, lease_time=5.0)
        registry.register_semaphore("reg_only", redis_client, limit=10, lease_time=5.0)

        governor = FakeGovernor(default_limit=10)
        governor.set_limit("shared", 999)  # 同名不同 limit，不应覆盖 registry
        governor.set_limit("gov_only", 5)

        manager = GovernorManager(governor)
        all_status = await manager.get_all_resources()

        assert set(all_status.keys()) == {"shared", "reg_only", "gov_only"}
        assert all_status["shared"]["limit"] == 30  # registry 胜出
        assert all_status["reg_only"]["limit"] == 10
        assert all_status["gov_only"]["limit"] == 5
        # unified "resource" key
        for name, status in all_status.items():
            assert status["resource"] == name

class TestSyncGetLimitTolerance:
    async def test_sync_get_limit_works(self):
        """v0.1.6 pinning: a governor whose get_limit is synchronous must be
        queryable (isawaitable tolerance)."""
        if HAS_TASKBASE:
            pytest.skip("taskbase installed: sync get_limit path is ambiguous")
        governor = FakeGovernor(default_limit=8)
        governor.set_limit("res_sync", 4)

        manager = GovernorManager(governor)
        status = await manager.get_resource_status("res_sync")

        assert status["limit"] == 4
        assert status["resource"] == "res_sync"
        assert status["usage"] == 0