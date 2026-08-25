
# orditect-flow 与 orditect-core 集成指南

## 集成概述

`orditect-flow` 可以独立使用，也可以与 `orditect-core`（资源治理框架）集成，获得分布式限流、连接池管理等能力。

## 集成方式

### 1. 使用 taskbase 的连接池

```python
from orditect.core import get_pool_manager

pool_manager = get_pool_manager()
redis_client = pool_manager.register_pool(
    "default",
    redis_url="redis://localhost:6379/0",
    max_connections=200,
)

```

### 2. 使用 taskbase 的资源治理

```python

from orditect.core import get_registry

registry = get_registry()
registry.register_semaphore(
    "llm",
    client=redis_client,
    limit=30,
    lease_time=30.0,
)

```

### 3. taskflow 自动检测并使用 taskbase

```python

from orditect.flow import get_default_storage, get_default_governor

# 自动使用 taskbase 的 TaskRedisDB
storage = get_default_storage(redis_client)
if hasattr(storage, 'connect'):
    await storage.connect()

# 自动使用 taskbase 的 AsyncLeaseSemaphore
governor = get_default_governor(redis_client)

```

### 4. 创建编排器

python

from orditect.flow import TaskOrchestrator

orchestrator = TaskOrchestrator(storage, governor)



## 集成优势

| 特性 | 独立使用 | 与 taskbase 集成 |
|------|---------|----------------|
| **任务存储** | RedisTaskStorage | TaskRedisDB（带状态索引、Lua 脚本） |
| **资源治理** | 本地信号量 | 分布式信号量（多实例共享） |
| **连接池** | 每个模块独立 | 统一连接池管理 |
| **适用场景** | 单实例部署 | 多实例部署、生产环境 |

## 完整集成示例

python

from orditect.core import get_pool_manager, get_registry
from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.flow.storage.factory import get_default_storage
from orditect.flow.governor.factory import get_default_governor

# 1. 初始化 taskbase
pool_manager = get_pool_manager()
redis_client = pool_manager.register_pool("default", "redis://localhost:6379/0")

registry = get_registry()
registry.register_semaphore("llm", client=redis_client, limit=30)

# 2. 初始化 taskflow（自动使用 taskbase）
storage = get_default_storage(redis_client)
if hasattr(storage, 'connect'):
    await storage.connect()

governor = get_default_governor(redis_client)

# 3. 创建编排器
orchestrator = TaskOrchestrator(storage, governor)

# 4. 定义任务
class LLMTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs):
        prompt = kwargs.get("prompt")
        result = await call_llm(prompt)
        return result

# 5. 提交任务（自动受 taskbase 的资源治理）
task = LLMTask(storage, governor)
task_id = await orchestrator.submit(
    task,
    prompt="What is AI?",
    resource="llm",  # 使用 taskbase 的 llm 资源池
)



## 最佳实践

1. **生产环境**：推荐使用 taskbase 集成，获得分布式限流和统一连接池管理
2. **开发环境**：可以独立使用 taskflow，简化依赖
3. **测试环境**：使用 UnlimitedGovernor，避免并发控制影响测试

## 故障排查

### 问题：taskflow 没有使用 taskbase

**原因**：`orditect-core` 未安装或未正确导入

**解决方案**：

bash

pip install orditect-core



### 问题：资源治理不生效

**原因**：未在 taskbase 中注册资源

**解决方案**：

python

registry = get_registry()
registry.register_semaphore("llm", client=redis_client, limit=30)