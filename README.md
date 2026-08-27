
# Orditect — Deterministic Governance for AI Workflows

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

**Orditect** is a production‑grade governance ecosystem for **AI workflows (LLM / Agent calls)**. It provides recursive composition, recovery, streaming output, and storage‑agnostic contracts — turning probabilistic AI execution into a deterministic, observable, and recoverable production capability.

---

## Core Capabilities

- 🏛️ **Recursive Composite Governance** – Any task can submit child tasks; lineage is auto‑registered, cancellation cascades, and resource exemptions prevent self‑deadlock.
- 🔄 **Recovery Plane** – Breakpoint‑resume and mid‑point rerun over the task tree, powered by `execution_id` generations and snapshot contracts.
- ⚖️ **Two‑Layer Governance + Budget** – Task‑level concurrency (`resource_type`) + call‑site governance (`GovernedClient`) + cross‑layer budget settlement (`BudgetLedger`) with hard‑stop guardrails.
- 📡 **Protocolised Streaming Output** – Golden‑frozen SSE protocol with rich‑media placeholders, multi‑stream mux, and disconnect strategies (`cancel` / `grace` / `continue`).
- 🔌 **Storage‑Replaceable Contracts** – Content, audit, result, and snapshot domains are fully decoupled via `orditect-protocol`; adapters plug in without touching the governance hot path.

> **Design Philosophy**: *Mechanisms to the framework, semantics to the business.* Vocabulary neutrality (T6), pointer discipline (T5), and terminal irreversibility (T3) are hard invariants. See [Design Principles](docs/PRINCIPLES.md).

---

## Packages

This repository is a **monorepo** of eight independently versioned Python
packages under the `orditect.*` namespace:

| Package | Description |
| :--- | :--- |
| [`orditect-core`](packages/core) | Governance engine – Redis + Lua task store, lease semaphore, token bucket, `reopen` primitive. |
| [`orditect-flow`](packages/flow) | Orchestration & recovery – recursive composition, cascade cancellation, `RecoveryService`, `GovernedCallClient`, `ActionDispatcher`. |
| [`orditect-stream`](packages/stream) | Output plane – SSE protocol, placeholders, mux, disconnect policies, FastAPI integration. |
| [`orditect-protocol`](packages/protocol) | Storage contracts – 5 domains, 10 protocols, 12 normative terms, conformance test kit. |
| [`orditect-adapter-memory`](packages/adapter-memory) | Reference adapter – in-memory implementation passing the full conformance suite. |
| [`orditect-adapter-local`](packages/adapter-local) | Local-file adapter – document-family reference, trace-bundle producer. |
| [`orditect-adapter-ui`](packages/adapter-ui) | UI adapter reference – trace-bundle consumer + action sink (HITL/MCP/agent). |
| [`orditect-bridge-openai`](packages/bridge-openai) | OpenAI-compatible endpoint bridge – governed LLM calls (producer tier reference). |
---

## Quick Start

```python
import redis.asyncio as aioredis
from orditect.core import TaskRedisDB, get_registry
from orditect.flow import BaseBackEndTask, TaskOrchestrator, get_default_storage, get_default_governor

# 1. Storage & governance
client = aioredis.from_url("redis://localhost:6379/0")
storage = get_default_storage(client)
await storage.connect()

registry = get_registry()
registry.register_semaphore("llm", client, limit=30)
governor = get_default_governor(client)

# 2. Orchestrator
orchestrator = TaskOrchestrator(storage, governor)

# 3. Define a task
class MyTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs):
        return {"result": f"processed {kwargs['data']}"}

# 4. Submit & wait
task = MyTask(storage, governor)
task_id = await orchestrator.submit(task, data={"key": "value"})
record = await orchestrator.wait_terminal(task_id, timeout=30)
print(record["status"], record.get("result"))
```

For streaming output, recovery, and adapter development, see the [documentation](#documentation) section.

---
## Three-Category Integration Example

```python
from orditect.adapter.local import LocalFileStore
from orditect.adapter.ui import TraceBundleReader, ActionSinkAdapter, MemoryActionQueue
from orditect.bridge.openai import GovernedLLMClient
from orditect.flow import BudgetLedger
from orditect.flow.actions import ActionDispatcher

# Storage adapter (trace bundle producer)
store = LocalFileStore("/var/lib/myapp/trace")

# Bridge (governed LLM calls)
llm = GovernedLLMClient(
    "https://api.openai.com",
    governor=governor, resource="llm",
    budget=ledger,
    audit_writer=store.audit,
    model="gpt-4o",
)

# UI adapter (consumer read + action sink)
reader = TraceBundleReader("/var/lib/myapp/trace")
queue = MemoryActionQueue()
sink = ActionSinkAdapter(queue)
dispatcher = ActionDispatcher(queue, orchestrator, recovery)

# Full governance loop
await dispatcher.start()
result = await llm.chat(messages=[...])  # sem + budget + audit + content
tree = await reader.snapshot.get_tree("root")  # observability
receipt = await sink.pause_node("task-123")  # HITL/MCP/agent intervention
```
See `docs/integration-guide.md` for the complete integration guide.

## Documentation

| Document | Description |
| :--- | :--- |
| [Design Principles](docs/PRINCIPLES.md) | The Three Core Contracts and cross‑cutting design invariants. |
| [Roadmap](docs/ROADMAP.md) | Upcoming milestones, version philosophy, and what we will not do. |
| [Protocol Terms](packages/protocol/docs/terms.md) | Normative T‑terms (T1–T11) with enforcement and verification. |
| [Core Lua Contract](packages/core/docs/lua_contract.md) | Frozen ARGV specs for all 10 Lua scripts. |
| [Flow Recovery](packages/flow/docs/recovery.md) | Resume / rerun design, execution dispatch, and pause semantics. |
| [Stream Protocol](packages/stream/docs/protocol.md) | SSE event schema, cancel sequences, and pause/resume decisions. |
| [Adapter Guide](packages/protocol/README.md) | How to implement a storage adapter and run the conformance suite. |
| [Integration Guide](docs/integration-guide.md) | Three-category integration with certification checklist. |
---

## Installation

Each package is installed separately from PyPI (future) or directly from source:

```bash
pip install orditect-core orditect-flow orditect-stream orditect-protocol
# or from source:
pip install ./packages/core ./packages/flow ./packages/stream ./packages/protocol
```

Requires **Python 3.12+**.

---

## Contributing

We welcome contributions! Please read our [Contribution Guidelines](CONTRIBUTING.md) (if present) and the [Extension & Integration Discipline](docs/ROADMAP.md#extension--integration-discipline) before opening a PR.

All structural changes (models, Lua ARGV, terms) must follow the version review process and pass the conformance suite.

---

## License

This project is licensed under the **Apache‑2.0** License. See the [LICENSE](LICENSE) file for details.


# Orditect — AI 工作流的确定性治理框架

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

**Orditect** 是一套面向 **AI 工作流（LLM / Agent 调用）** 的生产级治理生态系统。它提供递归组合治理、恢复平面、流式输出协议和存储无关的契约层——将概率性的 AI 执行转化为确定性、可观测、可恢复的生产能力。

---

## 核心能力

- 🏛️ **递归组合治理** – 任意任务可提交子任务；谱系自动登记、级联取消、资源豁免防自死锁。
- 🔄 **恢复平面** – 基于 `execution_id` 代际和快照契约的断点续传（resume）与中间点重放（rerun）。
- ⚖️ **双层治理 + 预算** – 任务级并发（`resource_type`）+ 调用点治理（`GovernedClient`）+ 跨层预算结算（`BudgetLedger`），带硬顶拦截护栏。
- 📡 **协议化流式输出** – 冻结的 SSE 协议，支持富媒体占位符、多流复用和断连策略（`cancel` / `grace` / `continue`）。
- 🔌 **存储可替换契约** – 内容、审计、结果、快照四域通过 `orditect-protocol` 完全解耦；适配器接入无需触碰治理热路径。

> **设计哲学**：*机制归框架，语义归业务。* 词汇表中立（T6）、指针纪律（T5）、终态不可逆（T3）是硬性不变量。详见[设计原则](docs/PRINCIPLES.md)。

---

## 子包一览

本仓库是五个独立版本化 Python 包的 **monorepo**，共享 `orditect.*` 命名空间：

| 包 | 说明 |
| :--- | :--- |
| [`orditect-core`](packages/core) | 治理引擎 – Redis + Lua 任务存储、租约信号量、令牌桶、`reopen` 原语。 |
| [`orditect-flow`](packages/flow) | 编排与恢复 – 递归组合、级联取消、`RecoveryService` 恢复服务。 |
| [`orditect-stream`](packages/stream) | 输出面 – SSE 协议、占位符、多流复用、断连策略、FastAPI 集成。 |
| [`orditect-protocol`](packages/protocol) | 存储契约 – 4 个存储域、8 个协议接口、11 条规范条款、符合性测试套件。 |
| [`orditect-adapter-memory`](packages/adapter-memory) | 参考适配器 – 通过完整符合性套件的内存实现。 |

---

## 快速开始

```python
import redis.asyncio as aioredis
from orditect.core import TaskRedisDB, get_registry
from orditect.flow import BaseBackEndTask, TaskOrchestrator, get_default_storage, get_default_governor

# 1. 存储与治理
client = aioredis.from_url("redis://localhost:6379/0")
storage = get_default_storage(client)
await storage.connect()

registry = get_registry()
registry.register_semaphore("llm", client, limit=30)
governor = get_default_governor(client)

# 2. 编排器
orchestrator = TaskOrchestrator(storage, governor)

# 3. 定义任务
class MyTask(BaseBackEndTask):
    async def execute(self, task_id: str, **kwargs):
        return {"result": f"processed {kwargs['data']}"}

# 4. 提交并等待
task = MyTask(storage, governor)
task_id = await orchestrator.submit(task, data={"key": "value"})
record = await orchestrator.wait_terminal(task_id, timeout=30)
print(record["status"], record.get("result"))
```

流式输出、恢复平面和适配器开发详见[文档导航](#文档导航)章节。

---

## 文档导航

| 文档 | 说明 |
| :--- | :--- |
| [设计原则](docs/PRINCIPLES.md) | 三大底层契约与贯穿全栈的设计不变量。 |
| [路线图](docs/ROADMAP.md) | 后续里程碑、版本理念及明确不做的事项。 |
| [协议条款](packages/protocol/docs/terms.md) | 规范级 T 条款（T1–T11），含执行约束与验证方式。 |
| [Core Lua 契约](packages/core/docs/lua_contract.md) | 全部 10 个 Lua 脚本的冻结 ARGV 规格。 |
| [Flow 恢复平面](packages/flow/docs/recovery.md) | Resume / rerun 设计、执行派发与暂停语义。 |
| [Stream 协议](packages/stream/docs/protocol.md) | SSE 事件模式、取消序列与暂停/恢复决策。 |
| [适配器开发指南](packages/protocol/README.md) | 如何实现存储适配器并运行符合性套件。 |

---

## 安装

各包可独立从 PyPI（未来）或源码安装：

```bash
pip install orditect-core orditect-flow orditect-stream orditect-protocol
# 或从源码安装：
pip install ./packages/core ./packages/flow ./packages/stream ./packages/protocol
```

要求 **Python 3.12+**。

---

## 参与贡献

欢迎贡献！提交 PR 前请阅读[贡献指南](CONTRIBUTING.md)（如有）和[扩展与集成纪律](docs/ROADMAP.md#扩展与集成纪律)。

所有结构性变更（模型、Lua ARGV、条款）必须遵循版本评审流程，并通过符合性测试套件。

---

## 许可证

本项目采用 **Apache‑2.0** 许可证。详见 [LICENSE](LICENSE) 文件。