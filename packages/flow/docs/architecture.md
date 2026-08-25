
# orditect-flow 架构设计

## 整体架构


┌─────────────────────────────────────────────────────────┐
│                    FastAPI Application                   │
│  ┌───────────────────────────────────────────────────┐  │
│  │           TaskOrchestrator（任务编排器）            │  │
│  │  - submit() / get_status() / cancel()             │  │
│  └───────────────────────────────────────────────────┘  │
│                          ↓                               │
│  ┌───────────────────────────────────────────────────┐  │
│  │           TaskExecutor（任务执行器）                │  │
│  │  - 获取资源 / 执行任务 / 处理异常 / 释放资源        │  │
│  └───────────────────────────────────────────────────┘  │
│                          ↓                               │
│  ┌───────────────────────────────────────────────────┐  │
│  │           TaskLifecycle（生命周期管理）             │  │
│  │  - 初始化 / 状态流转 / 取消 / 查询                 │  │
│  └───────────────────────────────────────────────────┘  │
│                          ↓                               │
│  ┌───────────────────────────────────────────────────┐  │
│  │           TaskStateMachine（状态机）                │  │
│  │  - 状态流转校验 / 终态判断                         │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                          ↓ 依赖抽象接口
┌─────────────────────────────────────────────────────────┐
│              TaskStorageProtocol（存储接口）              │
│  - initialize_task() / update_task() / get_task()        │
└─────────────────────────────────────────────────────────┘
                          ↓ 实现
┌─────────────────────────────────────────────────────────┐
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │DefaultStorage│  │RedisStorage  │  │TaskRedisDB   │  │
│  │(简单实现)     │  │(优化实现)     │  │(taskbase)    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘

code


## 核心设计原则

### 1. 单一职责原则（SRP）

每个组件职责单一：
- **TaskOrchestrator**：负责任务编排
- **TaskExecutor**：负责任务执行
- **TaskLifecycle**：负责生命周期管理
- **TaskStateMachine**：负责状态流转
- **TaskStorageProtocol**：负责任务存储
- **ResourceGovernorProtocol**：负责资源治理

### 2. 依赖倒置原则（DIP）

依赖抽象接口，而不是具体实现：
- `TaskOrchestrator` 依赖 `TaskStorageProtocol`（抽象接口）
- `TaskExecutor` 依赖 `ResourceGovernorProtocol`（抽象接口）
- 具体实现可以替换（如从 Redis 换成 PostgreSQL）

### 3. 开闭原则（OCP）

对扩展开放，对修改关闭：
- 通过插件式架构支持扩展（Callback、Scheduler、RetryPolicy）
- 新增功能不需要修改核心代码

### 4. 组合优于继承

通过组合实现功能，而不是继承：
- `TaskOrchestrator` 组合 `TaskExecutor` 和 `TaskLifecycle`
- `CompositeCallback` 组合多个 `Callback`

## 状态机设计

### 状态流转图


pending → queued → running → succeeded
                    ↓
                cancelled
                    ↓
                 failed

code


### 状态说明

| 状态 | 说明 | 是否终态 |
|------|------|---------|
| **pending** | 任务已创建，等待调度 | ❌ |
| **queued** | 任务已入队，等待执行 | ❌ |
| **running** | 任务正在执行 | ❌ |
| **succeeded** | 任务成功完成 | ✅ |
| **failed** | 任务执行失败 | ✅ |
| **cancelled** | 任务被取消 | ✅ |

### 状态流转规则

- `pending` → `queued` / `cancelled`
- `queued` → `running` / `cancelled`
- `running` → `succeeded` / `failed` / `cancelled`
- 终态（`succeeded` / `failed` / `cancelled`）不可再流转

## 工作流编排设计

### DAG（有向无环图）

工作流使用 DAG 管理步骤依赖关系：

```python
workflow = Workflow(
    name="document_processing",
    steps=[
        WorkflowStep(name="parse", handler=parse_document),
        WorkflowStep(name="chunk", handler=chunk_text, dependencies=["parse"]),
        WorkflowStep(name="embed", handler=generate_embeddings, dependencies=["chunk"]),
    ],
)
```


**DAG 特性**：
- 拓扑排序：自动确定执行顺序
- 循环依赖检测：防止死锁
- 并行执行识别：同一级别的步骤可以并行执行

### Saga 模式（失败回滚）

工作流失败时，自动逆序调用已完成步骤的回滚函数：

```python

WorkflowStep(
    name="upload_file",
    handler=upload_file,
    rollback_handler=upload_file_rollback,  # 失败时回滚
)

```

**回滚顺序**：
1. 工作流执行失败
2. 逆序遍历已完成步骤
3. 调用每个步骤的 `rollback_handler`
4. 回滚失败不影响其他步骤的回滚

## 松散耦合设计

### 抽象接口

`orditect-flow` 通过抽象接口实现松散耦合：

```python

# 存储接口（taskflow 定义，taskbase 实现）
class TaskStorageProtocol(Protocol):
    async def initialize_task(self, task_id: str, initial_status: str): ...
    async def update_task(self, task_id: str, updates: dict): ...
    async def get_task(self, task_id: str) -> dict: ...

# 资源治理接口（taskflow 定义，taskbase 实现）
class ResourceGovernorProtocol(Protocol):
    async def acquire(self, resource: str, timeout: float) -> str: ...
    async def release(self, resource: str, token: str) -> None: ...
```


### 自动检测

`get_default_storage()` 和 `get_default_governor()` 会自动检测 `orditect-core`：

```python

def get_default_storage(redis_client):
    try:
        from orditect.core import TaskRedisDB
        return TaskRedisDB(client=redis_client)  # 使用 taskbase
    except ImportError:
        return RedisTaskStorage(redis_client)  # 使用默认实现

```

## 性能优化

### 1. 状态索引

`RedisTaskStorage` 使用状态索引加速列表查询：

```python

# 状态索引：taskflow:status:pending -> {task_id1, task_id2, ...}
# 查询 pending 状态的任务时，直接从索引获取，无需 SCAN
```


### 2. Pipeline 原子操作

使用 Redis Pipeline 保证原子性：

```python

async with self.redis.pipeline(transaction=True) as pipe:
    pipe.set(task_key, data)
    pipe.sadd(status_key, task_id)
    await pipe.execute()
```


### 3. 批量查询

使用 MGET 批量获取任务记录：

```python

task_keys = [self._make_task_key(tid) for tid in task_ids]
raws = await self.redis.mget(task_keys)
```


## 扩展性设计

### 插件式架构

通过插件式架构支持扩展：

- **Callback**：Webhook、WebSocket、自定义回调
- **Scheduler**：Priority、Cron、Delayed、Dependency
- **RetryPolicy**：自定义重试策略
- **BackoffStrategy**：自定义退避策略

### 自定义实现

可以替换任何组件的实现：

```python

# 自定义存储
class PostgreSQLTaskStorage(TaskStorageProtocol):
    async def initialize_task(self, task_id: str, initial_status: str):
        # PostgreSQL 实现
        pass

# 自定义资源治理
class KubernetesResourceGovernor(ResourceGovernorProtocol):
    async def acquire(self, resource: str, timeout: float):
        # Kubernetes 实现
        pass
```


## 与 orditect-core 的协同

### 集成架构

```code

┌─────────────────────────────────────────┐
│  orditect-flow（编排层）               │
│  - BaseBackEndTask / Orchestrator        │
│  - Workflow / Retry / Callback           │
└─────────────────────────────────────────┘
              ↓ 可选依赖（松散耦合）
┌─────────────────────────────────────────┐
│  orditect-core（数据面）               │
│  - TaskRedisDB / AsyncLeaseSemaphore     │
│  - RedisPoolManager / LimiterRegistry    │
└─────────────────────────────────────────┘
```


### 协同优势

- **任务存储**：taskbase 的 TaskRedisDB 提供状态索引、Lua 脚本等优化
- **资源治理**：taskbase 的 AsyncLeaseSemaphore 提供分布式限流
- **连接池**：taskbase 的 RedisPoolManager 提供统一连接池管理

## Budget audit sink relocation (v0.1.0)

`BudgetAuditSink` (record_charge: scope / call_id / units / balance_after)
is generalized into the orditect-protocol **audit domain**
(`AuditWriter.append(AuditEvent)`), where the four fields map onto:

- `event_type = "budget_charge"`
- `event_id = call_id`        (idempotency key, T4)
- `scope = scope`
- `payload = {"units": ..., "balance_after": ...}`

Flow keeps `BudgetAuditSink` as a thin backward-compatible adapter that
translates record_charge into an AuditEvent before delegating to the
protocol AuditWriter. No behavior change for existing callers.

