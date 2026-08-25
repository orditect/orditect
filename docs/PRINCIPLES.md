# Orditect Design Principles

This document captures the foundational design principles that govern the entire Orditect ecosystem. They are the invariant rules that every module, adapter, and integration must respect, and they shape the boundaries between the framework and business semantics.

---

## The Three Core Contracts

These are the non‑negotiable architectural boundaries that define what Orditect is—and, equally important, what it is not.

### 1. Governance‑as‑Mechanism, Not Orchestration (Orchestration Independence)

Orditect does not define workflows, schedule task order, or parse DAG dependencies. It answers only one question:

> *“Given a task node, what is its current state, and how should it transition to the next state?”*

**Permitted:**
- Atomic state operations: `initialize_task`, `update_task`, `reopen_task`, `get_task`.
- Recovery helpers: `resume` / `rerun` (scope explicitly provided by the caller).
- Read‑only lineage inspection: `build_tree()` (no business semantics).
- Dependency governance APIs that are passive: `register` / `get` / `vote` / `notify`.

**Forbidden:**
- Any built‑in scheduler, router, conditional branching, or DAG logic in the framework.
- Automatic inference of dependency‑driven reruns (e.g., “if A depends on B, rerun B must rerun A”).
- Automatic dependency‑ready driving inside the executor (this is the caller’s domain).

### 2. Hot‑Storage Boundary: Redis for Transient State, Cold Data Externalised (Hot/Cold Separation)

Redis stores only the transient state of in‑flight tasks. Snapshots, audits, results, and content bodies are managed by external adapters through the `orditect-protocol` contracts.

**Permitted:**
- Redis stores `task_id`, `status`, `execution_id`, `remaining_deps`, `active_children`.
- Semaphore slots, token‑bucket counters, lease metadata.
- References to external content via `TaskPointer`.

**Forbidden:**
- Storing content bodies larger than 1 KB in Redis.
- Framework‑default behaviour that automatically restores expired hot records from cold storage.
- Framework‑level routing decisions between hot and cold stores.

### 3. Deployment Neutrality: Governance Kernel Runs Locally, Data Sovereignty Stays with the Customer

The governance kernel does not depend on any cloud service. It runs entirely in the customer’s own VPC, on‑premises data centre, or even a single Docker container.

**Permitted:**
- Deployment on customer‑owned Redis instances.
- Adapter‑driven synchronisation of cold data to external stores (PG / MinIO / Milvus) – the customer chooses the data location.
- Optional SaaS control planes for cross‑project cold‑data aggregation – but never required for core governance.

**Forbidden:**
- Any reliance on cloud APIs or external network services for the governance hot path.
- Default upload of governance data to the author’s cloud services.
- Any “cloud‑first” default configuration baked into the framework.

---

## Cross‑Cutting Design Principles

### Mechanism vs. Semantics (Vocabulary Neutrality – T6)

The framework deliberately embeds **no business vocabulary**. All statuses, event types, backend identifiers, and scopes are opaque strings. The caller declares:
- terminal status sets (`terminal_statuses` / `transitions`) per framework instance,
- success words for recovery (`reuse_terminal_words` – empty set rejects construction),
- task reconstruction (`task_factory`) – the framework never knows how to rebuild a task from its ID.

### Pointer Discipline (T5)

Redis hot records carry only **pointers**, never content bodies. Content exceeding a threshold must be pointerised into a `TaskPointer(backend, key, metadata)`. The “content‑before‑pointer” ordering guarantees that any recorded pointer always resolves.

### Lease Model (T1) + Terminal Irreversibility (T3) + Generation Identity (T11)

- Every lifecycle is expressed as an **absolute expiry instant** `expire_at` (timezone‑aware UTC). Reads filter lazily.
- Within a single execution generation (`execution_id`), a terminal state is **unconditionally irreversible** – the Lua layer rejects any mutation, bypassing Python‑side validation.
- “Rerun” is not a state regression; it is a **`reopen_task` that opens a new generation**. The old generation remains untouched (terminal protection still holds for it), and the new generation is a fresh lifecycle.
- `execution_id` is aligned across **three projections** (T11):
  - **Core hot record**: assigned at creation (`initialize_task`), advanced by `reopen_task`.
  - **Flow execution**: every snapshot reads the current `execution_id`.
  - **Protocol snapshot domain**: versions are keyed by `(task_id, step, execution_id)`.
  Any divergence corrupts time‑travel.

### Observation Non‑Blocking (T9)

All observation writes (sinks, hooks, audits) are wrapped in try/except by the caller – a failure only logs, never blocks the business path. Implementations must raise only subclasses of `ContractError`, ensuring total handling.

### Explicit Capability (T8)

Adapters declare their supported half‑domains via `CapabilitySet`. Invoking an undeclared capability must raise `UnsupportedCapabilityError` – **silent no‑ops and fake successes are forbidden**.

### Bounded Waiting

The framework contains **no infinite waits**. Every blocking point has a timeout:
- `settle_timeout`, `grace_period`, `post_cancel_drain_timeout`,
- `wait_terminal(timeout)`, `acquire_timeout`.

### Idempotency & Concurrency Atomicity (T4, T10)

Every write interface exposes an explicit idempotency key:
- Audit: `event_id`
- Snapshot: `(task_id, step, execution_id)`
- Result: `stream_id`
- Budget: `call_id` (dual‑habitat key)

Same key + identical payload = silent dedup.  
Same key + different payload = explicit `IdempotencyConflictError`.  
Check‑and‑write operations are atomic inside a single Lua script – the TOCTOU window is closed.

---

## Architectural Implications

These principles lead to a clear separation of responsibilities:

| Layer | Responsibility | Owns |
|---|---|---|
| **Framework (core / flow / stream)** | Mechanisms – state machines, semaphores, lineage, recovery, SSE protocol | Governance correctness |
| **Protocol (`orditect-protocol`)** | Storage contracts – data models, interfaces, conformance suite | Interoperability |
| **Adapters (open‑source / commercial)** | Concrete storage backends – content, audit, result, snapshot | Data durability & query capabilities |
| **Business / Product Layer** | Semantics – vocabulary, task factory, version guards, RBAC, HITL, MCP tools | Customer‑facing experience |

By honouring these boundaries, the ecosystem remains extensible, auditable, and upgrade‑safe without forking or monkey‑patching core internals.

---

*This document is the authoritative reference for design decisions. For term‑by‑term enforcement, refer to `orditect-protocol/docs/terms.md`. For version‑specific architecture details, see each package’s `docs/` folder.*