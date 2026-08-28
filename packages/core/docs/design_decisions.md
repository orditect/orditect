# Orditect Core — Design Decisions

This document records the architectural decisions that shape orditect-core.
Each entry states the decision, its rationale, and the condition that would
trigger re-evaluation. It is a living document: decisions are revised only
with a version bump and a synchronous update of this file.

For the storage-side term contracts (T1–T12), see
`orditect-protocol/docs/terms.md`. For Lua script call specifications, see
`docs/lua_contract.md`.

---

## DD-001: fail-open / fail-close explicit policy

**Decision.** A policy enum `on_unavailable: Literal["fail_open",
"fail_close"]` is attached to limiter construction, defaulting to
`fail_close` (raises `LimiterUnavailableError`).

Semantics:
- `fail_close`: when Redis is persistently unavailable, acquire raises
  (safety first — prefer refusing service over losing governance).
- `fail_open`: when Redis is persistently unavailable, acquire proceeds
  (availability first — local degraded permission).

**Current state.** `LimiterUnavailableError` is reserved in `errors.py`;
the policy interface is not yet implemented.

**Trigger for implementation.** Must be implemented and rehearsed before
production (including a degradation drill runbook).

---

## DD-002: Cluster key discipline

**Current state.** `task_update` / `quota` scripts are multi-key and would
fail cross-slot in Cluster mode. The semaphore already uses the `{ftb}`
hashtag (free cluster slot compatibility).

**Discipline** (when Cluster support is needed):
- task domain: hashtag by `{tenant}` (e.g. `task:{tenant_123}:task_456`)
- quota domain: hashtag by `{scope}` (e.g. `admission:{scope_abc}:pending_units`)
- semaphore: keep `{ftb}` (already compatible)

**Trigger for implementation.** A real Cluster deployment requirement.

---

## DD-003: quota long-task renewal

**Current state.** When a task outlives its `task_ttl`, its quota lease
would be reaped by the crash-recovery logic. The current discipline is
"set TTL large" (e.g. 7 days), which covers the vast majority of cases.
The idempotent-renewal path (a retry IS a renewal — see `quota_reserve.lua`)
already keeps a retried task's lease alive.

**Future option.** A quota heartbeat renewal API isomorphic to the
semaphore watchdog (periodically refreshing the ZSET score via a
quota_refresh script).

**Trigger for implementation.** A real scenario with both "long tasks
(> 7 days)" and "tight quotas (requiring precise reclamation)".

---

## DD-004: no FIFO-fair semaphore

**Current state.** The ZSET scheme is approximate "first-come-first-served"
and does not guarantee strict FIFO. In batch scenarios, starvation is
theoretically possible (small late tasks never winning slots held by long
tasks).

**Decision.** Explicitly not doing it. This is a theoretical concern; a
LIST + blocking-queue scheme will only be added if a real starvation
scenario appears.

---

## DD-005: no generic secondary-index framework

**Current state.** The lineage index (`task_children`) is a concrete,
need-driven implementation.

**Decision.** Do not abstract it into a generic index framework. Multi-domain
isolation is handled by the existing `task_key_prefix` parameterization (one
prefix per domain — dataset, agent, etc.); no kind/type field is added.

---

## DD-006: no pub/sub cancel push

**Current state.** The `CancellationToken` polling cache (100ms window)
already covers the performance need.

**Decision.** Do not introduce a pub/sub push model (the connection
management complexity is not worth it).

---

## DD-007: index lifecycle — member-level lease vs key-level TTL

**Decision.** Shared indexes (status / lineage) use the ZSET lease model
(member = task_id, score = expire_at), with key-level TTL increase-only as a
residue fallback only.

**Rationale.** With a shared collection whose members have independent TTLs,
a single key-level TTL is mathematically unsolvable — any value either kills
active members by contagion or leaves ghost members accumulating. Member-level
expiry instants are the only correct answer, and they are isomorphic to the
semaphore/quota lease primitives and to the taskstore relational row-level
`expire_at` model.

**Rejected alternative.** The "index and primary record share a key-level TTL"
contract — it only holds when an index is exclusively owned by one task and
breaks as soon as multiple tasks share it.

---

## DD-008: deterministic ID convention

**Decision.** Cross-framework entity IDs are deterministically generated
(e.g. stream's enrich task_id = `enrich-{placeholder_id}`), so the dispatcher
and the reference side align with zero channel.

**Rationale.** Idempotency requires a stable ID — deterministic IDs let
retries/replays naturally fall into the store's "unique key + ON CONFLICT"
idempotency primitive, with no extra mapping table (the chain IS the query).
Random IDs + a back-channel would require maintaining a placeholder →
task_id mapping, introducing write-timing and consistency windows (the same
class of race as the TOCTOU bugs already fixed).

**Cost and mitigation.** It occupies the task_id namespace — mitigated by a
framework prefix discipline (the `enrich-` prefix is framework-reserved;
business must not occupy it).

---

## DD-009: reopen primitive vs terminal protection

**Decision.** Breakpoint-resume and mid-point rerun are implemented via
`reopen_task` (`task_reopen.lua`) — a "controlled new generation opening"
primitive, NOT a state transition.

**Rationale.** Terminal protection (T3) and the rerun requirement are in
structural conflict: terminal irreversibility requires the Lua layer to
reject any overwrite unconditionally, while rerun essentially re-executes a
terminal node. Opening a backdoor in terminal protection for rerun (e.g.
allowing specific transition regressions) would corrode the core invariant.
The solution separates "rerun" from "state transition":
- Within one generation: terminal protection holds unconditionally (any
  overwrite is rejected).
- Reopen: a terminal task opens a new generation under a new execution_id,
  with state reset to initial — the old generation's record is NOT modified
  (terminal protection still holds for it), and the new generation is a
  fresh lifecycle.

**execution_id three-way alignment (T11)**:
- core hot record: reopen_task writes the new execution_id;
- flow execution: every execute / resume / rerun uses the current
  execution_id;
- protocol snapshot: versions are keyed by (task_id, step, execution_id).
Any divergence between the projections corrupts time-travel.

**Where old-generation data goes.** Redis hot records keep only the latest
generation plus a `previous_execution_ids` trace array (capped at 50). The
full history is written by the flow snapshot sink into the protocol snapshot
domain (cold storage such as PostgreSQL). Redis carries no history burden —
consistent with "Redis for transient state, cold data externalised": Redis
addresses the present; history belongs to cold storage.

**Who generates execution_id.** Python-side (`exec-{uuid4hex[:12]}`), passed
into Lua — consistent with the task_id passing discipline, avoiding random
source assembly inside Lua. execution_id is a runtime-generated value and is
NOT part of the deterministic-ID convention (that convention constrains
task_id only).

**Status.** Implemented (the reopen primitive ships with the recovery plane).

---

## DD-010 (formerly DD-013): MCP direction isolation — hot/cold path separation

**Decision.** Orditect adopts a hot/cold-separated MCP strategy rather than
wholesale rejection or embrace of MCP In.

### Absolute exclusion zone (MCP In forbidden from the hot path)

| Zone | Rationale |
|---|---|
| Atomic state transitions (task_update) | Lua scripts must complete atomically server-side; any external (including MCP) involvement breaks atomicity and introduces TOCTOU (see T4/T10) |
| Semaphore acquire/release | Semaphore operations complete in microseconds with a fixed watchdog renewal cadence; MCP's millisecond latency directly causes lease misjudgement |
| `reopen_task` primitive | Must complete atomically in a single script, guaranteeing terminal irreversibility (T3) and exactly-one-winner under concurrent reopen (T10) |
| Budget charging (`BudgetLedger.charge`) | The call_id dual-habitat idempotency key's check-and-write must complete atomically inside Redis; no external system may participate in the decision |

### Allowed scenarios

**MCP Out (read-only queries + asynchronous triggers) — the default
production mode.** MCP acts as an "operations panel": agents observe and make
macro decisions through MCP but never intervene in an executing task flow
(lineage queries, audit-log pulls, asynchronous rerun triggers landing in a
queue, budget-usage queries).

**MCP In (read-only analysis + offline ring) — development / debugging /
evolution scenarios only.** MCP In runs only off the production hot path:
task replay and reproduction in development, quality supervision and
reconciliation, flow-evolution experiments in a sandbox, and candidate
version recording for human review. Hard constraints:
- Physical isolation first: MCP In agent operations route to a SEPARATE
  Redis instance (e.g. db 14/15) or a local file adapter, never sharing
  storage with the production hot path.
- Mandatory environment tagging: all MCP requests carry an environment
  header (production / development / sandbox); MCP In requests against
  production are unconditionally rejected at the gateway layer.
- Zero hot-path dependency: orditect-core's Lua scripts and Redis operations
  introduce NO MCP client dependency; MCP adapters interact with core only
  through orditect-protocol interfaces, read-only.
- Bypass principle: any "write" an agent performs via MCP In (e.g.
  triggering a rerun) lands in a queue, pulled asynchronously by the
  dispatcher at the next cycle. An agent never calls `task_update.lua`
  directly.

**Rationale.** (1) Performance determinism: the governance SLA (< 50ms)
cannot depend on LLM inference latency (500ms–2s). (2) Reliability
decoupling: the hot path survives any external model-service outage.
(3) Cost control: high-frequency state transitions consume no tokens.
(4) The offline ring is exactly where the envisioned self-evolution
capability lives — agents reading historical snapshots, analyzing failure
patterns, and experimenting with workflow variants while the system is idle.
This is what distinguishes Orditect from a pure governance framework.

**Consequences.** Positive: Orditect is among the very few frameworks
offering both "sub-millisecond atomic governance" and "offline-ring agent
self-evolution". Negative: it cannot support "agent dynamically adjusting
parameters of a running task" (e.g. interrupting an LLM stream, mutating an
in-flight task's quota) — judged an unnecessary capability architecturally,
substituted by node-boundary cancel + rerun (see DD-008 in the flow docs:
pause semantics = cancel + resume, no standalone suspend state).

**Risk.** Customers may mistakenly assume "MCP In works in the sandbox, so
it works in production too". Product documentation must explicitly state
"MCP In is forbidden on the production hot path; violations may cause task
state inconsistency", with gateway-layer enforcement.

---

## Related documents

- `docs/lua_contract.md` — frozen KEYS/ARGV specs of all Lua scripts.
- `orditect-protocol/docs/terms.md` — the T1–T12 storage-side term contracts
  (T3 terminal irreversibility, T4 idempotency, T6 vocabulary neutrality,
  T9 observation non-blocking, T10 concurrency atomicity, T11 execution
  identity alignment).
- flow docs — orchestration-layer decisions (pause semantics = cancel +
  resume; orchestration independence).