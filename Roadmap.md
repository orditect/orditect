# Orditect Roadmap

> This document outlines the planned evolution of the Orditect project. It is a communication tool for the project's direction, not a rigid commitment. It will be updated quarterly based on real-world feedback. Last updated: 2026-08-24.

---

## Mission

**Provide deterministic governance for the inherent uncertainty of AI workflows.**

At its core, AI is a probabilistic engine – as long as LLMs output probability distributions, task failures, resource leaks, and state drifts are inevitable. Orditect does not attempt to eliminate uncertainty; it provides a rigid, observable, recoverable, and auditable foundation for that uncertainty.

**We open-source the protocol; we close-source the experience.**

- **Open Source (The Protocol):** The atomic governance primitives (`core` + `protocol`) – state machines, semaphores, idempotency, and the reopen primitive. These are the deterministic anchor. They are auditable, forkable, and re-implementable in any language.
- **Close Source (The Experience):** Engineering adapters (PG/MinIO/Milvus) + the governance knowledge base. The former is the engineering complexity for high-scale production; the latter is the set of rules and patterns distilled from real-world failure modes. Both appreciate in value with every deployment.

---

## Current Status (v0.1.0 Alpha)

| Module | Status | Notes |
| :--- | :--- | :--- |
| `orditect-protocol` | ✅ Implemented | Four domains, 8 protocols, 11 specification clauses, conformance suite baseline. |
| `orditect-core` | ✅ Implemented | Redis + Lua atomic operations, semaphores, lease model, `reopen` primitive. |
| `orditect-flow` | ✅ Implemented | Recursive composition, cascading cancellation, resource exemption, `RecoveryService`. |
| `orditect-stream` | ✅ Implemented | Golden SSE protocol, rich media placeholders, disconnect strategies. |
| `orditect-adapter-memory` | ✅ Implemented | Reference implementation, passes conformance suite. |
| Conformance Suite Coverage | ⚠️ Baseline | 2-5 cases per domain; will expand with PG adapter development. |
| `DelayedScheduler` | ⚠️ Skeleton | Interface only; production use requires APScheduler/Celery. |
| SSE journal replay | ⚠️ Reserved | Protocol anchor frozen; replay logic not yet implemented. |
| Suspend/Pause mechanism | 📋 v0.2.0 | Current semantic: pause = cancel + resume; v0.2.0 will introduce vocabulary-neutral suspend. |

---

## Phase 0: Protocol Stabilization & Adapter Ecosystem Launch (Now → 2026-Q4)

**Objective**: Validate the atomic governance protocol in real scenarios, accumulate initial case studies, and back-validate the conformance suite.

| Milestone | Acceptance Criteria |
| :--- | :--- |
| **Protocol Public Release** | `core` + `protocol` published as independent open-source project with README, ROADMAP, CONTRIBUTING. |
| **Local Adapter Usable** | `adapter-memory` passes full conformance suite; developers can spin up a demo environment in 5 minutes. |
| **Suite Expansion** | Append new conformance cases (pagination, sorting, aggregate precision) as the first real backend (PG) is developed. |
| **Health Check Validation** | Read-only governance observability + 7-day health report generation runs on at least 3 real AI task trees. |

**Delivery Artifacts**:
- Public GitHub repository (protocol + reference implementation).
- PyPI placeholder packages (`orditect`, `orditect-core`, `orditect-protocol`).
- Docker Compose one‑click experience environment.

**Priority Note**: Local adapter comes before any cloud SaaS offering. The hot path must always run inside the user’s VPC; SaaS will only be a convenience layer for cold data aggregation, never a necessity for governance capability.

---

## Phase 1: Treatment Modules (2026-Q4 → 2027-Q1)

**Objective**: Turn core diagnostic capabilities into deliverable treatment solutions.

| Milestone | Acceptance Criteria |
| :--- | :--- |
| **Permanent Result Reuse** | Business-layer archive proxy solves result reuse after hot‑record TTL expiry (Plan B first, zero framework modifications). |
| **Version Guard Factory** | Task reconstruction verifies code version; fail‑safe block on mismatch, with audit trail. |
| **PostgreSQL Adapter (Closed Source)** | Passes full conformance suite; becomes the second real backend for protocol 1.0 freeze. |
| **Read‑only Visualization** | Render lineage trees, version lists, and aggregate statistics; purely consumes the snapshot domain. |

**Delivery Artifacts**:
- `orditect-adapter-pg` (commercial wheel distributed via private PyPI).
- Read‑only visualization panel (embeddable or standalone).
- Governance health report template.

**Architectural Discipline**: All modules above are wired via public injection points (sink/query/task_factory/duck‑typing). Zero monkey‑patching, zero framework forking.

---

## Phase 2: Commercial Closed‑Source Layer (2027-Q1 → 2027-Q3)

**Objective**: Form the complete commercial loop of “open protocol + closed experience”.

| Milestone | Acceptance Criteria |
| :--- | :--- |
| **HITL Operational Surface** | Human intervention via approval node modeling; suspend semantics align with v0.2.0 suspend mechanism. |
| **MinIO/Milvus Adapters** | Content persistence + vector retrieval (private interfaces, outside contract). |
| **Governance Knowledge Base v1** | Rule set distilled from case studies (anomaly thresholds, rerun strategy recommendations, budget templates). |
| **MCP Out Read‑only Interface** | Agents can query lineage trees, audit logs, version diffs (no hot‑path involvement). |

**Delivery Artifacts**:
- Complete closed‑source adapter suite (PG + MinIO + Milvus).
- Governance knowledge base (encrypted rule set, versioned).
- MCP tool set (read‑only + asynchronous triggers, no hot‑path interception).

**Commercial Note**: The closed‑source layer provides incremental value in engineering convenience and data‑derived intelligence. It never withholds any core governance capability from the open‑source version.

---

## Phase 3: Diagnostic & Evolution Capabilities (2027-Q3 → 2027-Q4)

**Objective**: Build the MCP Out offline layer into an AI‑assisted governance diagnosis and self‑healing system.

| Milestone | Acceptance Criteria |
| :--- | :--- |
| **Anomaly Detection Model** | Unsupervised learning on historical traces to auto‑tag nodes with potential resource leaks or performance drift. |
| **Root Cause Analysis Assistant** | Multi‑agent collaboration (logs/traces/metrics) produces diagnostic reports with accuracy comparable to SOTA. |
| **Evolution Sandbox** | Isolated environment to replay historical tasks, compare version diffs, and generate optimization recommendations. |
| **Self‑healing Pilot** | Agent triggers rerun experiments in the offline layer; improvements are adopted after human confirmation. |

**Delivery Artifacts**:
- Governance diagnostic service (sidecar process, non‑blocking to hot path).
- Evolution sandbox (physically isolated Redis + snapshots).
- Governance knowledge base v2 (including fine‑tuned model weights).

**Key Design Principle**: All algorithms/models/AI capabilities strictly live in the offline layer. The hot path remains algorithm‑free, LLM‑free, and maintains sub‑millisecond latency. The diagnostic service communicates with the governance engine only via read‑only metadata references and asynchronous webhooks – never blocks the main flow.

---

## What We Will NOT Do (Equally Important)

The following are outside the roadmap and will not be added even if requested:

| Boundary | Rationale |
| :--- | :--- |
| **Algorithms/LLM on the hot path** | The governance engine must have a deterministic SLA (< 10 ms) with zero external dependencies. Algorithms are confined to the MCP Out offline layer. |
| **MCP In on the hot path** | Agent involvement in state transitions would introduce unpredictable latency. MCP is for read‑only queries and asynchronous triggers only (see DD‑013). |
| **Business semantics in the framework** | Vocabulary neutrality is a hard rule. All statuses, event types, and scopes are treated as opaque strings, injected by the upper layer. |
| **Carrying payload content in Redis** | Redis hot records store only pointers; content is managed by adapters via the protocol layer (T5). |
| **Replacing existing Agent frameworks** | Orditect does not compete with LangChain/LangGraph/DeepAgent. It embeds at their call boundaries to provide governance. |

---

## Version Plan

| Version | Target | Core Content |
| :--- | :--- | :--- |
| **v0.1.0** | 2026-Q3 | Protocol, core, flow, stream, memory adapter, conformance baseline. |
| **v0.2.0** | 2026-Q4 | Vocabulary‑neutral suspend, suite expansion, PG adapter proposal. |
| **v0.3.0** | 2027-Q1 | Permanent result reuse archive, version guard, read‑only visualization. |
| **v1.0.0** | 2027-Q2 | Protocol freeze, second real backend (PG) passes full suite, commercial adapters released. |
| **v1.1.0** | 2027-Q3 | HITL operational surface + governance knowledge base v1. |
| **v2.0.0** | 2027-Q4 | MCP Out diagnostics + evolution sandbox. |

---

## Commercial Options (Optional, Not Part of Open‑Source Core)

The following roadmap items are independent of the open‑source protocol layer. They are additional choices for production users and do not affect the completeness of the open‑source version.

| Module | Distribution Form | Description |
| :--- | :--- | :--- |
| **PostgreSQL Adapter** | Closed‑source wheel | Long‑term snapshot/audit persistence; open‑source users may implement via JDBC. |
| **MinIO Adapter** | Closed‑source wheel | Content pointer storage; open‑source users may implement via S3 API. |
| **Governance Knowledge Base** | Closed‑source rule set | Thresholds and policies derived from real failure cases; pure experience asset. |
| **Enterprise Support** | SLA agreement | Dedicated architect response, priority bug fixes, custom adapter development. |

**Important Clarifications**:
- All closed‑source modules are clients of the open‑source protocol, not extensions of it.
- Any progress in the open‑source version (e.g., v0.2.0 suspend) is reflected in the commercial edition.
- Community contributions are accepted under Apache 2.0 and will never be incorporated into closed‑source modules.

---

## How to Contribute

We welcome contributions in the following areas:

1. **Protocol improvements**: Lua script optimizations, additional conformance cases, new storage backend protocol support.
2. **Adapter ecosystem**: Re‑implement the protocol in any language, develop new open‑source adapters.
3. **Diagnostic experience**: Share real‑world AI system failure stories to help evolve the governance knowledge base.

**Contribution Path**:
- Read `protocol/docs/terms.md` to understand the 11 specification clauses.
- Run the conformance suite to ensure changes don’t break contracts.
- Any structural change (model fields, Lua ARGV) must follow the version review process.

---

*This roadmap is updated quarterly. Discussion via Issues and PRs is always welcome.*