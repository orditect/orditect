

"""StreamGovernorManager 单元测试（升级清单任务 1：sem 可视化增强）。

覆盖：
- taskbase registry 优先路径：已注册资源经 registry 查询，
  返回格式统一（"resource" key，内部从 taskbase "name" 转换）
- 降级路径：注入的 governor 鸭子类型扩展（get_limit/list_resources）
- 单资源不存在：ValueError
- get_all_resources：registry + governor 来源合并，governor 不覆盖 registry

注意：
- taskstream 的 ResourceGovernorProtocol 未定义 get_limit/list_resources，
  降级路径是鸭子类型探测（本文件 LocalGovernor 模拟 taskflow
  DefaultResourceGovernor 这类带扩展的实现）
- taskbase 相关用例：未安装时自动 skip；registry 路径需要真实 Redis
  （semaphore.in_use() 触网），复用 tests/conftest.py 的 redis_client fixture
"""
import pytest

from orditect.stream.governor import StreamGovernorManager

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

class LocalGovernor:
    """带鸭子类型扩展（get_limit/list_resources）的本地 governor。

    模拟 taskflow DefaultResourceGovernor 这类实现：
    协议方法之外提供状态查询所需的扩展接口（同步 list_resources）。
    """

    def __init__(self, default_limit: int = 10):
        self.default_limit = default_limit
        self._limits: dict[str, int] = {}
        self._usage: dict[str, int] = {}
        self._counter = 0

    def set_limit(self, resource: str, limit: int) -> None:
        self._limits[resource] = limit

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        self._counter += 1
        self._usage[resource] = self._usage.get(resource, 0) + 1
        return f"local-token-{self._counter}"

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self._usage[resource] = max(0, self._usage.get(resource, 0) - 1)

    async def get_usage(self, resource: str) -> int:
        return self._usage.get(resource, 0)

    async def get_limit(self, resource: str) -> int:
        return self._limits.get(resource, self.default_limit)

    def list_resources(self) -> list[str]:
        """同步实现：验证 isawaitable 兼容路径。"""
        return list(self._limits)


class BareGovernor:
    """裸 protocol 实现（无 get_limit/list_resources 扩展）。"""

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        return "bare-token"

    async def try_acquire(self, resource: str) -> str | None:
        return "bare-token"

    async def release(self, resource: str, token: str) -> None:
        pass

    async def get_usage(self, resource: str) -> int:
        return 0


# ---------- degraded path (pure in-memory, no taskbase/Redis needed) ----------

class TestStreamGovernorManagerFallback:
    """降级路径：registry 未命中时直接查询注入的 governor。"""

    async def test_get_resource_status_from_local_governor(self):
        """鸭子类型扩展的资源可查询，返回五字段统一格式。"""
        governor = LocalGovernor()
        governor.set_limit("vector_search", 5)

        manager = StreamGovernorManager(governor)
        status = await manager.get_resource_status("vector_search")

        assert status == {
            "resource": "vector_search",
            "limit": 5,
            "usage": 0,
            "available": 5,
            "utilization": "0.0%",
        }

    async def test_fallback_usage_reflects_acquire(self):
        """降级路径 usage 反映实际获取量。"""
        governor = LocalGovernor()
        governor.set_limit("res_a", 2)

        token = await governor.acquire("res_a")
        try:
            manager = StreamGovernorManager(governor)
            status = await manager.get_resource_status("res_a")
            assert status["usage"] == 1
            assert status["available"] == 1
            assert status["utilization"] == "50.0%"
        finally:
            await governor.release("res_a", token)

    async def test_fallback_unknown_resource_uses_default_limit(self):
        """未 set_limit 的资源：get_limit 回退 default_limit。"""
        if HAS_TASKBASE:
            pytest.skip("taskbase installed: default_limit fallback path is ambiguous")
        governor = LocalGovernor(default_limit=7)
        manager = StreamGovernorManager(governor)

        status = await manager.get_resource_status("never_seen")
        assert status["limit"] == 7
        assert status["usage"] == 0

    async def test_resource_not_found_raises(self):
        """registry 与 governor 均无此资源（裸实现）：ValueError。"""
        manager = StreamGovernorManager(BareGovernor())

        with pytest.raises(ValueError, match="Resource not found"):
            await manager.get_resource_status("ghost")

    async def test_get_all_resources_empty(self):
        """无已知资源时返回 {}。"""
        manager = StreamGovernorManager(BareGovernor())
        assert await manager.get_all_resources() == {}

    async def test_get_all_resources_from_governor(self):
        """降级聚合：list_resources() 枚举的资源全部返回（同步实现兼容）。"""
        if HAS_TASKBASE:
            pytest.skip("taskbase installed: governor-only aggregation is ambiguous")
        governor = LocalGovernor()
        governor.set_limit("res_a", 3)
        governor.set_limit("res_b", 5)

        manager = StreamGovernorManager(governor)
        all_status = await manager.get_all_resources()

        assert set(all_status.keys()) == {"res_a", "res_b"}
        assert all_status["res_a"]["limit"] == 3
        assert all_status["res_a"]["resource"] == "res_a"
        assert all_status["res_b"]["limit"] == 5


# ---------- taskbase registry priority path ----------

@requires_taskbase
class TestStreamGovernorManagerTaskbase:
    """registry 优先路径（需要 orditect-core + 真实 Redis）。

    semaphore.in_use() 需访问 Redis 清理过期 token，
    本组测试复用 tests/conftest.py 的 redis_client fixture（db15，跑前清场）。
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
        registry.register_semaphore(
            "default_stream_llm", redis_client, limit=30, lease_time=5.0
        )

        # same resource in governor has different limit → should return registry's 30, proves priority
        governor = LocalGovernor()
        governor.set_limit("default_stream_llm", 999)

        manager = StreamGovernorManager(governor)
        status = await manager.get_resource_status("default_stream_llm")

        assert status == {
            "resource": "default_stream_llm",
            "limit": 30,
            "usage": 0,
            "available": 30,
            "utilization": "0.0%",
        }

    async def test_registry_usage_reflects_acquire(self, redis_client):
        """registry 路径 usage 反映实际获取量。"""
        registry = get_registry()
        sem = registry.register_semaphore(
            "default_stream_llm", redis_client, limit=2, lease_time=5.0
        )

        token = await sem.acquire(timeout=1.0)
        try:
            manager = StreamGovernorManager(governor=None)
            status = await manager.get_resource_status("default_stream_llm")
            assert status["usage"] == 1
            assert status["available"] == 1
            assert status["utilization"] == "50.0%"
        finally:
            await sem.release(token)

    async def test_unregistered_falls_back_to_governor(self, redis_client):
        """registry 未注册的资源：降级到 governor 查询。"""
        governor = LocalGovernor()
        governor.set_limit("local_only", 4)

        manager = StreamGovernorManager(governor)
        status = await manager.get_resource_status("local_only")

        assert status["resource"] == "local_only"
        assert status["limit"] == 4

    async def test_get_all_resources_merges_sources(self, redis_client):
        """聚合：registry 为主 + governor 补充，governor 不覆盖 registry。"""
        registry = get_registry()
        registry.register_semaphore("shared", redis_client, limit=30, lease_time=5.0)
        registry.register_semaphore("reg_only", redis_client, limit=10, lease_time=5.0)

        governor = LocalGovernor()
        governor.set_limit("shared", 999)  # 同名不同 limit，不应覆盖 registry
        governor.set_limit("gov_only", 5)

        manager = StreamGovernorManager(governor)
        all_status = await manager.get_all_resources()

        assert set(all_status.keys()) == {"shared", "reg_only", "gov_only"}
        assert all_status["shared"]["limit"] == 30  # registry 胜出
        assert all_status["reg_only"]["limit"] == 10
        assert all_status["gov_only"]["limit"] == 5
        # unified "resource" key
        for name, status in all_status.items():
            assert status["resource"] == name