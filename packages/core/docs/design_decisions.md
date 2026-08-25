# taskbase 设计决策（W4：只定方向，不写实现）

本文档记录需要明确方向但当前不实现的设计决策。触发实现的条件已标注。

## 1. fail-open / fail-close 显式策略（原 P2-D2）

**决策**：定义策略枚举 `on_unavailable: Literal["fail_open", "fail_close"]`，
挂在 limiter 构造参数上，默认 `fail_close`（抛 `LimiterUnavailableError`）。

**语义**：
- `fail_close`：Redis 持续不可用时，acquire 抛错（安全优先，宁可拒绝服务）
- `fail_open`：Redis 持续不可用时，acquire 放行（可用性优先，本地降级许可）

**当前状态**：`LimiterUnavailableError` 已预留（errors.py），策略接口未实现。

**触发实现的条件**：上生产前必须实现并演练（含降级演练手册）。

## 2. Cluster 键规范

**现状**：task_update / quota 脚本均为多键脚本，Cluster 模式下跨槽失败。
信号量已用 `{ftb}` hashtag（免费 cluster 槽位兼容）。

**规范**（未来支持 Cluster 时）：
- task 域：按 `{tenant}` 做 hashtag（如 `task:{tenant_123}:task_456`）
- quota 域：按 `{scope}` 做 hashtag（如 `admission:{scope_abc}:pending_units`）
- 信号量：沿用 `{ftb}`（已兼容）

**触发实现的条件**：出现 Cluster 部署需求时。

## 3. quota 长任务续租

**现状**：任务时长 > task_ttl 时，配额会被崩溃回收逻辑误清。
当前纪律是"TTL 设大"（如 7 天），覆盖绝大多数场景。

**未来方案**：与 watchdog 同构的 quota 心跳续租 API
（周期性调用 quota_refresh.lua 刷新 ZSET score）。

**触发实现的条件**：出现"长任务（>7 天）+ 紧配额（需要精确回收）"真实场景。

## 4. FIFO 公平队列信号量

**现状**：ZSET 方案是"先到先得"的近似公平，不保证严格 FIFO。
批处理场景可能出现饥饿（后来的小任务永远抢不到被长任务占满的槽位）。

**决策**：明确不做。理论问题，遇到真实饥饿场景再加（LIST + 阻塞队列方案）。

## 5. 通用二级索引框架

**现状**：B2 的谱系索引（task_children）是具体需求驱动的实现。

**决策**：不抽象成通用索引框架。多域隔离用现有的 `task_key_prefix` 参数化
（dataset 域一套前缀、agent 域一套前缀），不加 kind/type 字段。

## 6. pub/sub 主动取消推送

**现状**：B6 的 CancellationToken 轮询缓存（100ms 窗口）已覆盖性能需求。

**决策**：不引入 pub/sub 推送模型（连接管理复杂度不值）。



---

### 6. 移交清单（taskflow 阶段二需要的全部接口签名）
# taskbase v0.3.0 → taskflow 移交清单

本文档汇总 taskflow 阶段二接线所需的全部 taskbase v0.3.0 接口签名。

## TaskRedisDB 状态机宿主化

```python
from fastapi_taskbase import TaskRedisDB

# taskflow 接入时声明自己的词汇表
db = TaskRedisDB(
    redis_url="...",
    terminal_statuses=("succeeded", "failed", "cancelled"),  # taskflow 终态集合
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

## 谱系索引（R6 级联取消的数据基础）

# 父任务 submit 子任务时登记谱系
await db.initialize_task(
    task_id="child_task",
    parent_task_id="parent_task",  # 谱系登记
)

# 查询某任务的全部子任务
children = await db.list_children("parent_task")  # -> ["child_task", ...]

## 幂等初始化（防父任务重试抹回子任务状态）
# 父任务重试重新 submit 子任务时，已存在则跳过
ok = await db.initialize_task(
    task_id="child_task",
    if_not_exists=True,  # 幂等开关
)
# ok=False 表示任务已存在（未覆盖）

## 原子 JSON merge（替代非原子 RMW）
db = RedisDB(redis_url)
await db.connect()

# 原子 merge（并发安全）
await db.update("some_key", {"field": "value"})
# key 不存在时抛 KeyError

## CancellationToken 轮询缓存
from fastapi_taskbase import CancellationToken

token = CancellationToken(
    task_id="task_123",
    task_redis_db=db,
    min_interval=0.1,  # 轮询缓存窗口（秒），0=禁用
)

# 长流程分段检查
await token.raise_if_cancelled()

## 状态索引规模化读取
# 过滤幽灵成员 + 分批读取
ids = await db.list_task_ids_by_status(
    status="pending",
    filter_expired=True,  # 过滤任务 key 已过期但索引残留的成员
    limit=1000,  # 最多返回数量
)


## Lua 脚本调用契约

详见 `docs/lua_contract.md`（v0.3.0 冻结的 ARGV 规格）。

## 设计决策

详见 `docs/design_decisions.md`（fail-open/close、Cluster 键规范、quota 续租等）。

## 7. 索引生命周期：成员级租约 vs key 级 TTL（v0.3.2 决策）

**决策**：共享索引（状态/谱系）采用 ZSET 租约模型
（member=task_id, score=expire_at），key 级 TTL 只增不减仅作防残留兜底。

**理由**：共享集合 + 成员独立 TTL 场景下，单点 key TTL 数学上无解——
任何取值都会让一部分成员"被连坐失踪"或"幽灵滞留"。
成员级到期时刻是唯一正解，且与 sem/quota 的租约原语同构、
与 taskstore 的 PG 行级 expire_at 模型同构（见 taskstore_backlog.md §5）。

**放弃的方案**："索引与主记录 key 级同 TTL"契约（v0.3.1 T9）——
只在单任务独享索引时成立，多任务共享即破，已翻转钉扎。

## 8. 确定性 ID 约定（v0.3.2 框架规范）

**决策**：跨框架协作的实体 ID 采用确定性生成
（如 taskstream enrich 任务的 task_id = `enrich-{placeholder_id}`），
派发方与引用方零通道对齐。

**理由**：
- 幂等前提是 ID 稳定——确定性 ID 让重试/重放天然落入 taskstore
  的"唯一键 + ON CONFLICT"幂等原语，无需额外映射表（链即查询）。
- 随机 ID + 回传通道需要维护 placeholder → task_id 映射，
  引入写入时机与一致性窗口（与已修掉的 TOCTOU 同类竞态源）。

**代价与消解**：占用 task_id 命名空间——以框架前缀规范消解
（`enrich-` 前缀框架保留，业务不得占用）。



---

## 最终验证

```bash
# 1. 全量测试
pytest tests/ -v

# 预期：96 passed, 2 skipped

# 2. 打包验证
pip install -e .

# 3. 版本号确认
python -c "import fastapi_taskbase; print(fastapi_taskbase.__version__)"
# 预期输出：0.3.0

# 4. Lua 脚本入包确认
python -c "from fastapi_taskbase._lua import load_lua; print(load_lua('json_merge.lua')[:50])"
# 预期输出：-- json_merge.lua —— 通用原子 JSON merge...
```
## 9. reopen 原语与终态保护的关系（v0.1.0 决策）

**决策**：断点续传/中间点重跑通过 `reopen_task`（task_reopen.lua）实现——
它是"开新 execution 代"的独立原语，不是状态流转。

**理由**：终态保护（T3）与重跑需求存在结构性冲突——终态不可逆要求
Lua 无条件拒绝覆盖，而重跑本质是让终态节点重新执行。若为重跑给终态
保护开后门（如允许特定流转回退），核心不变量即被腐蚀。
解法是把"重跑"从"状态流转"中区分出来：
- 同代内：终态保护无条件生效（任何覆盖被拒）；
- reopen：终态任务开启新 execution_id 的一代，状态重置为初始——
  旧代记录不被修改（终态保护对其仍成立），新代是全新生命周期。

**execution_id 三方对齐（T11）**：
- core 热记录：reopen_task 写入新 execution_id；
- flow 执行：每次 execute / resume / rerun 使用当前 execution_id；
- protocol 快照：按 (task_id, step, execution_id) 列版本。
任何一处口径不同，回溯张冠李戴。

**旧代数据去向**：Redis 热记录只保留最新代 + previous_execution_ids
留痕数组（上限 50）。完整历史轨迹由 flow 快照 sink 写入 protocol
快照域（PG 等冷存储）。Redis 不背历史包袱——与"Redis 中心性 +
指针纪律"一致：Redis 管当前态寻址，历史态归冷存储。

**execution_id 生成方**：Python 侧生成（exec-{uuid4hex[:12]}）传入 Lua——
与 task_id 传入范式一致，避免 Lua 内拼凑随机源；execution_id 是运行时
生成值，不参与确定性 ID 约定（确定性约定只约束 task_id）。

**触发实现的条件**：本版本（v0.1.0）随恢复体系落地。

---

## `design_decisions.md` 补充条目

### DD-013: MCP 方向隔离策略——热路径/冷路径分离

**决策日期**：2026-08-23

**状态**：已批准 / 长期有效

---

#### 背景

MCP（Model Context Protocol）正在成为 Agent 与外部系统交互的事实标准。大厂（Microsoft、Google、Anthropic、腾讯等）普遍将 MCP 作为 Agent 参与工作流执行的主要通道（即“MCP In”模式）。

但 MCP In 的本质是：**每一次工具调用或状态查询，都需经过 LLM 进行意图识别、参数生成和结果解析**。这意味着：

- **延迟不可预测**：LLM 推理延迟通常在 500ms-2000ms，且随上下文长度超线性增长
- **可靠性依赖外部**：模型服务宕机、限流、超时会直接阻断工作流执行
- **成本随调用量线性增长**：高频状态流转（如信号量 acquire/release）若走 MCP，Token 消耗将不可承受

Orditect 的核心资产是 **Redis + Lua 驱动的确定性状态机**，它要求亚毫秒级响应和绝对的原子性保证。因此，需要明确 MCP 在 Orditect 架构中的边界。

---

#### 决策

**Orditect 采用“热路径/冷路径分离”的 MCP 策略，而非一刀切地拒绝或拥抱 MCP In。**

##### 一、绝对禁区（MCP In 禁止进入 Orditect 热路径）

以下场景**严禁**通过 MCP 进行任何形式的 Agent 介入：

| 禁区 | 说明 | 理由 |
| :--- | :--- | :--- |
| **状态机原子流转** | `task_update.lua` 中涉及状态变更、终态校验、索引迁移的所有操作 | Lua 脚本必须在 Redis 服务端原子完成，任何外部（含 MCP）的介入都会破坏原子性，引入 TOCTOU 漏洞（见 T4/T10） |
| **信号量 acquire/release** | `AsyncLeaseSemaphore` 的租约获取与释放 | 信号量操作必须在微秒级完成，watchdog 续约周期固定，MCP 的毫秒级延迟会直接导致租约误判超时 |
| **`reopen_task` 原语** | 任何涉及 `execution_id` 推进的操作 | 必须由 Lua 单脚本原子完成，确保终态不可逆（T3）和并发 reopen 恰好一个胜者（T10） |
| **预算扣费（`BudgetLedger.charge`）** | `call_id` 双栖幂等键的检查与写入 | 必须在 Redis 内原子完成，不允许外部系统参与决策 |

##### 二、允许场景（MCP In / MCP Out 均可，但职责不同）

###### MCP Out（只读查询 + 异步触发）—— 生产环境默认模式

MCP 作为 **“操作面板”**，Agent 通过 MCP 进行观测和宏观决策，但不干预正在执行的任务流：

| 场景 | 说明 | 接口示例 |
| :--- | :--- | :--- |
| **谱系查询** | Agent 读取任务树、节点状态、资源分配 | `orditect_get_tree(task_id)` |
| **审计日志拉取** | 查询历史操作记录、错误栈、cost 明细 | `orditect_get_audit_log(task_id, time_range)` |
| **异步触发重跑** | Agent 发起 `rerun` 指令，写入任务队列后立即返回 | `orditect_execute_rerun(task_id, scope)` |
| **预算余额查询** | Agent 读取 `BudgetLedger` 余额，辅助资源调度决策 | `orditect_get_budget_usage(project_id)` |

###### MCP In（只读分析 + 离线圈层）—— 开发/调试/进化场景

MCP In **仅允许在非生产热路径**中运行，即系统闲置时、开发测试环境、或专门隔离的“进化沙箱”中：

| 场景 | 说明 | 接口示例 | 运行环境约束 |
| :--- | :--- | :--- | :--- |
| **任务重放与复现** | Agent 在开发环境中读取快照，重建执行上下文并进行调试 | `orditect_replay_execution(execution_id)` | 必须标记 `X-Environment: development`，禁止在生产 namespace 执行 |
| **质量监督与对账** | Agent 对比多次执行结果（如 v1 vs v2），分析回归 | `orditect_diff_executions(exec_id_1, exec_id_2)` | 仅允许读取快照域，不允许写入 |
| **流程进化实验** | Agent 在沙箱中运行变异后的工作流，对比历史基线 | `orditect_experiment_run(workflow_variant, baseline_id)` | 必须在独立 Redis DB（如 db 14）中运行，与生产数据物理隔离 |
| **多次版本结果记录** | Agent 将实验产生的快照作为“候选版本”写入版本库，供人工评审 | `orditect_submit_candidate_version(snapshot)` | 写入专用的 `candidate_snapshots` 表，不污染生产 `execution_id` 序列 |

---

#### 架构落地原则

1. **物理隔离优先**：MCP In 相关的 Agent 操作，默认路由至 **独立的 Redis 实例（db 14/15）** 或 **Local File Adapter**，绝不与生产热路径共享存储。
2. **环境标记强制**：所有 MCP 请求必须携带 `X-Orditect-Environment` 头（`production` / `development` / `sandbox`）。Production 环境的 MCP In 请求被网关层**无条件拒绝**。
3. **热路径零依赖**：`orditect-core` 的所有 Lua 脚本及 Redis 操作，**不引入任何 MCP 客户端依赖**。MCP Adapter 只能通过 `orditect-protocol` 定义的接口与 Core 交互，且仅限于读操作。
4. **旁路原则**：Agent 通过 MCP In 进行的任何“写入”操作（如触发 rerun），一律落入 `DelayedScheduler` 队列，由 Core 在下一个调度周期异步拉取执行。**不允许 Agent 直接调用 `task_update.lua`**。

---

#### 理由

1. **性能确定性**：Orditect 的 SLA 承诺治理引擎响应时间 < 50ms，热路径不依赖 LLM 延迟是这一承诺的前提。
2. **可靠性解耦**：热路径不依赖任何外部模型服务，即使 OpenAI/Anthropic 宕机，Orditect 的级联取消和状态机依然能正常工作。
3. **成本可控**：高频状态流转不消耗 Token，治理本身不产生额外 LLM 费用。
4. **进化能力的载体**：MCP In 在离线圈层中的使用，恰好承载了你设想的 **“自我进化”** 能力——Agent 在系统闲置时读取历史快照、分析失败模式、实验新工作流变体——这正是 Orditect 区别于纯治理框架的独特价值。

---

#### 后果

- **正面**：Orditect 成为极少数能够在生产环境中提供 **“亚毫秒级原子治理 + 离线圈层 Agent 自主进化”** 双模能力的框架。
- **负面**：无法支持“Agent 在运行中动态调整当前任务参数”的交互模式（如中断 LLM 流式输出、修改进行中的任务配额）。但这在架构上被判定为“不必要的能力”，可通过节点边界取消 + 重跑的模式替代（见 DD-008：暂停语义 = 取消 + 恢复）。
- **风险**：客户可能误以为“MCP In 在沙箱中能跑，所以在生产环境也能跑”。需要在产品文档中明确标注 **“生产环境热路径禁止 MCP In，违规操作可能导致任务状态不一致”**，并提供网关层强制拦截。

---

#### 相关决策

- DD-008: 暂停语义 = 取消 + 恢复（无独立挂起态）
- DD-002: 词汇表中立（T6）
- DD-009: 观测非阻塞（T9）
- T3: 终态不可逆
- T4: 幂等与并发原子性
- T10: 并发原子性（恰好一个胜者/干净合并）
- T11: 执行身份对齐（execution_id 三方一致）

---

**文档维护者**：Orditect 架构组
**最后更新**：2026-08-23
**下一次评审**：v0.2.0 版本发布前