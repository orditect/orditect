```markdown
# Project Roadmap

> **Disclaimer**: This document reflects the current thinking of the Orditect maintainers. It is a **communication tool** for the project’s direction, not a rigid commitment. Priorities may shift based on real‑world feedback and community contributions. The roadmap is reviewed quarterly.

---

## Current Status (v0.1.0 Alpha)

The core governance machinery is **implemented and stable**:

| Module | Status |
| :--- | :--- |
| `orditect-protocol` | ✅ Implemented – 5 domains, 10 protocols, 12 normative terms, conformance suite baseline |
| `orditect-core` | ✅ Implemented – Redis + Lua atomic operations, lease semaphore, token bucket, `reopen` primitive |
| `orditect-flow` | ✅ Implemented – Recursive composition, cascading cancellation, resource exemption, `RecoveryService` |
| `orditect-stream` | ✅ Implemented – SSE protocol, multi‑stream mux, placeholders, disconnect strategies |
| `orditect-adapter-memory` | ✅ Implemented – Reference implementation passing the conformance suite |
| Conformance suite coverage | ⚠️ Baseline – 2–5 cases per domain; will expand with the first production backend |
| `orditect-flow` dependency governance | ✅ Implemented (v0.1.1) – passive multi-parent dependency APIs (register / ready / vote / notify), exemption snapshot, offline cycle scan & counter rebuild |
> **Known skeletons / placeholders**: `DelayedScheduler`, `DeadLetterQueue.retry()`, SSE journal replay, and the vocabulary‑neutral suspend mechanism are explicitly reserved for future iterations (see below).

---

## Upcoming Milestones

### 1. Protocol Stabilisation & Adapter Ecosystem (Towards v1.0)

**Goal**: Freeze the storage contracts and establish a robust adapter ecosystem.

- **PostgreSQL Adapter (commercial / closed‑source)** – The first production‑ready relational backend.
  - Must pass the full conformance suite (the *second* real backend to do so, which is the 1.0 freeze criterion).
  - Expands conformance cases for pagination, sorting, aggregation precision, and edge‑case concurrency.
- **Local File Adapter (open‑source)** – Zero‑dependency persistence for development and small‑scale deployments.
  - Allows running the full recovery plane without any external infrastructure.
- **Protocol v1.0** – Declared stable; terms become immutable. Future changes require a major version bump.

### 1b. Adapter / Bridge Ecosystem for Dependency Governance

- `orditect-adapter-local` / `orditect-adapter-pg` cold `dep_graph_store`
  implementations (full dependency-graph persistence).
- `orditect-bridge-*` packages wiring external orchestration frameworks
  (LangChain / LangGraph / ...) to `DependencyGovernor`'s passive APIs.

### 2. Operational Surfaces (HITL & MCP)

**Goal**: Turn governance primitives into actionable interfaces for both humans and agents.

- **Human‑in‑the‑Loop (HITL) operational surface**
  - Approval‑node modelling pattern (recommended: model approvals as child tasks).
  - Align with the forthcoming vocabulary‑neutral suspend mechanism (v0.2.0 iteration).
- **MCP‑based Agent interface (read‑only + asynchronous triggers)**
  - Agent queries: lineage tree, audit logs, version diffs, resource usage.
  - Trigger operations: `rerun`, `resume`, `cancel` – always via asynchronous, non‑blocking scheduling.
  - **Critical guardrails**: budget hard‑stop (via `BudgetLedger`) and full auditability (via `AuditEvent`).
  - **Strict separation**: MCP In never touches the governance hot path (see DD‑013).

### 3. Production‑Grade Observability & Diagnostics

**Goal**: Provide built‑in or easily‑pluggable diagnostics for AI workflow health.

- **Read‑only lineage visualisation** – Tree rendering, version lists, aggregate statistics (consume snapshot domain).
- **Governance health reports** – 7‑day summaries of semaphore usage, task failure patterns, budget consumption.
- **Pluggable hooks** – Prometheus / Langfuse / OpenTelemetry via the existing `LimiterHooks`, `StreamHooks`, and task lifecycle callbacks.

### 4. Content & Multi‑Modal Adapters

**Goal**: Expand storage coverage beyond relational.

- **MinIO / S3 adapter** – Content‑pointer persistence for large binary objects.
- **Milvus / Vector adapter** – Vector storage and similarity search (explicitly outside the protocol’s query scope – these are private adapter interfaces).

### 5. Offline Diagnostics & AI‑Assisted Evolution

**Goal**: Transform governance data into intelligent diagnostics and automated optimization recommendations – strictly in the **offline layer**, never touching the hot path.

- **Anomaly detection model** – Unsupervised learning on historical traces to auto‑tag nodes with potential resource leaks or performance drift.
- **Root cause analysis assistant** – Multi‑agent collaboration (logs / traces / metrics) producing diagnostic reports.
- **Evolution sandbox** – Isolated environment to replay historical tasks, compare version diffs, and generate optimisation recommendations.
- **Self‑healing pilot** – Agent triggers rerun experiments in the offline layer; improvements are adopted after human confirmation.

---

## Commercial / Closed‑Source Options (Optional, Not Part of Open‑Source Core)

The following modules are independent additions on top of the open‑source protocol layer. They are **additional choices for production users** and do not affect the completeness of the open‑source version.

| Module | Distribution | Description |
| :--- | :--- | :--- |
| **PostgreSQL Adapter** | Closed‑source wheel | Long‑term snapshot / audit persistence; open‑source users may implement via JDBC. |
| **MinIO / S3 Adapter** | Closed‑source wheel | Content‑pointer storage; open‑source users may implement via the S3 API. |
| **Governance Knowledge Base** | Closed‑source rule set | Thresholds and policies distilled from real failure cases – pure experience asset. |
| **Enterprise Support** | SLA agreement | Dedicated architect response, priority bug fixes, custom adapter development. |

**Important clarifications**:
- All closed‑source modules are **clients** of the open‑source protocol, not extensions of it.
- Any progress in the open‑source version (e.g., v0.2.0 suspend) is reflected in the commercial edition.
- Community contributions (Apache 2.0) will never be incorporated into closed‑source modules.

---

## What We Will NOT Do (Equally Important)

These items are explicitly out of scope and will not be added to the core framework, even if requested:

| Boundary | Rationale |
| :--- | :--- |
| **Algorithms / LLM on the hot path** | The governance engine must have a deterministic SLA (< 10 ms) with zero external dependencies. Algorithms are confined to the offline / MCP‑Out layer. |
| **MCP In on the hot path** | Agent involvement in state transitions introduces unpredictable latency. MCP is for read‑only queries and asynchronous triggers only. |
| **Business semantics in the framework** | Vocabulary neutrality (T6) is a hard rule. All statuses, event types, and scopes are opaque strings injected by the upper layer. |
| **Carrying payload content in Redis** | Redis hot records store only pointers (T5); content is managed by adapters through the protocol layer. |
| **Replacing existing Agent frameworks** | Orditect does not compete with LangChain / LangGraph / DeepAgent. It embeds at their call boundaries to provide governance. |
| **Cloud‑first default configuration** | The governance kernel must run completely offline in the customer’s own environment. SaaS is optional and only for cold‑data aggregation. |

---

## Version Philosophy

| Version | Focus |
| :--- | :--- |
| **v0.1.x** | Core governance & recovery primitives; memory reference adapter; conformance baseline. |
| **v0.2.x** | Vocabulary‑neutral suspend mechanism; expanded conformance suite; first production adapter (PG). |
| **v1.0.0** | Protocol freeze. Requires a second real backend (PG) passing the full conformance suite. Adapter interfaces become stable; no breaking changes without major version bumps. |
| **v1.x+** | Operational surfaces (HITL, MCP), diagnostics, content adapters, and knowledge‑based governance rules (commercial/optional layers). |

---

## Extension & Integration Discipline

All extensions (adapters, observability, operational surfaces) must be implemented through **public injection points**:
- Sink / query interfaces (`snapshot_sink`, `snapshot_query`, `store`, `enricher`).
- Storage duck‑typing (`storage` proxy patterns).
- `task_factory` for task reconstruction.
- Gateway wrappers for RBAC / multi‑tenancy.

**Strictly forbidden in production**:
- Monkey‑patching internal methods (underscore‑prefixed `_*` methods).
- Overriding core methods in `executor.py` / `orchestrator.py`.
- Bypassing Lua scripts to craft `reopen` logic outside `task_reopen.lua` (breaks T3/T10 and invalidates conformance certification).

If a requirement hits a frozen contract boundary (e.g., adding a new structural field, relaxing a T‑term, or implementing a roadmap placeholder that belongs upstream), the correct path is an **upstream proposal** (model change → golden test update → terms.md review), not a hard‑wired workaround in the adapter layer.

---

## How You Can Help

We welcome contributions in these areas:

- **Protocol improvements**: Additional conformance cases, new storage domain protocols.
- **Adapter ecosystem**: Implement the protocol in other languages, or develop new open‑source adapters.
- **Diagnostic experience**: Share real‑world AI system failure stories to help evolve the governance knowledge base.

**Contribution path**:
1. Read `orditect-protocol/docs/terms.md` to understand the 11 normative terms.
2. Run the conformance suite to ensure changes don’t break contracts.
3. Open an issue or pull request – structural changes (model fields, Lua ARGV) must follow the version review process.

---

*This roadmap is updated quarterly. Discussion via Issues and PRs is always welcome.*
```