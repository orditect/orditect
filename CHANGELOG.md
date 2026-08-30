# Changelog

## [0.1.7] - TBD

**v0.1.7 = delivery + correctness release.**

### Fixed (delivery)

- **bridge-openai: declare `orditect-stream` as a runtime dependency** —
  `client.py` imports `orditect.stream.protocols.source`, but pyproject did
  not declare it; a standalone `pip install orditect-bridge-openai` broke on
  import with ModuleNotFoundError. The import-boundary gate now cross-checks
  every legal internal import against pyproject (an undeclared internal
  dependency is a gate failure), closing the blind spot that neither the
  gate nor the floors meta test could see.

### Fixed (stream output plane)

- **stream: SSE heartbeat postmortem — four independent defects behind one
  symptom**:
  - scheduling: heartbeat frames were only emitted right after a business
    event, so quiet periods (slow first token / enrich settle windows) left
    the connection idle for proxies to kill. The heartbeat is now produced
    by an independent coroutine on a fixed interval, merged with business
    frames through a local queue — it must never be scheduled on top of
    the runner's next-event wait, which is itself blocked during a quiet
    period;
  - configuration: `create_stream_response` hardcoded a 15.0s interval,
    silently ignoring the runner's `StreamConfig.heartbeat_interval`;
  - `StreamRunner.should_buffer` called the monitor's boolean property as
    a method (TypeError that silently blocked the grace-buffer path on
    disconnect — a latent defect found by the new pins);
  - the heartbeat comment frame carried a double prefix (": :ping"),
    unrecognizable to any consumer matching ":ping". Frame byte formats
    are now frozen by `tests/golden/test_frame_encoding.py`.
- **stream: `ManifestResolver` tolerates query-side exceptions** — the
  documented query contract ("missing task returns None") mismatches
  `TaskOrchestrator.get_task`, which raises TaskNotFoundError (v0.1.4
  contract). A resolver polling before the enrich task was submitted
  crashed through `_poll_task` and failed the whole `resolve_all` gather,
  taking every other placeholder down with it. Query exceptions are now
  treated as "not ready yet" (continue polling). New pins in
  `tests/unit/test_client_resolver.py`.

### Fixed (streaming governance)

- **flow / bridge-openai: aclose cascade on streaming paths** —
  `GovernedCallClient.call_streaming` and `GovernedLLMClient.stream` never
  closed their inner iterators (async-for never acloses), so a consumer
  break left the semaphore release and the HTTP connection cleanup to
  asyncio asyncgen finalization (GC timing). Both paths now hold the inner
  iterator and aclose it in finally — deterministically closing the httpx
  stream and running the full finally chain (finalize + shielded release)
  on every exit path. New pins.
- **core / flow: shielded-release discipline completed** (v0.1.6 CHANGELOG
  claim fulfilled): `@limited` had no strong-reference set at all (its
  release task could be GC-collected mid-release when the shield await was
  interrupted), and `GovernedClient._shielded_release` /
  `SemaphoreHold.__aexit__` lacked the RuntimeError fallback for
  loop-teardown windows that `TaskExecutor._shielded_finalize` already
  had. All three now share one discipline: create_task with RuntimeError
  fallback (close the coroutine, log, skip), strong-ref set with
  done-callback drain + exception retrieval. New pins.

## [0.1.6] - TBD

**v0.1.6 = correctness + hygiene release.** No new capabilities — every
change is a fulfillment of an existing contract promise, or a completion of
the certification/hygiene layer. Zero assertion flips; all pinning tests are
additive coverage of previously un-pinned paths.

### Fixed (correctness)

- **flow: F3 result-reuse ordering** — the reuse short-circuit now runs
  BEFORE semaphore acquire and BEFORE the running write. Previously a reused
  node (a) occupied a semaphore slot it never needed and (b) left the hot
  record stuck at RUNNING forever (wait_terminal would time out); on the
  default core wiring the branch was unreachable (Lua terminal protection
  rejected the running write first). New pinning tests in
  `test_result_reuse.py` (`TestResultReuseOrdering`).
- **flow: `scan_dependency_cycles` is iterative** — the offline cycle scan
  no longer recurses; a dependency chain deeper than Python's recursion
  limit (~1000) previously crashed the tool that exists to close the
  register-time DFS's depth-bound (32) blind spot. New pins for a 3000-deep
  acyclic chain and a >32-deep cycle.
- **core: `task_reopen.lua` clears `progress` / `cancel_outcome`** —
  generation-scoped settle metadata must not leak into a new generation
  (same rationale as the v0.1.5 result/error clear). `lua_contract.md`
  updated. New pinning test.
- **flow: `terminate` honors `request_cancel` and not-found races** —
  aligned with `cancel`'s sibling semantics: a refused request_cancel or a
  task vanishing mid-terminate now returns `False` instead of proceeding or
  leaking `TaskNotFoundError`. New pins.
- **bridge-openai: `_latency_ms` fully removed from the streaming path** —
  the result holder handed to cost_fn no longer carries the internal field
  (C5 completion). New pin.
- **adapter-ui: query kwargs are strict** — unknown keyword arguments now
  raise `TypeError` instead of being silently swallowed (T8 spirit); mixed
  datetime forms (seed datetime objects vs file ISO strings) are normalized
  so latest-generation folding/sorting stay coherent. New pins.
- **stream: `StreamResult` deduplicated** — `runner/stream.py` no longer
  re-defines the dataclass it also imports; executors return the canonical
  `stream.stream_result.StreamResult` (isinstance-safe across the
  runner/finalizer boundary). New pin.
- **flow: `charge()` surfaces a rejected quota write** — a rejected
  `reserve_units` is logged explicitly (audit still written; ledgers no
  longer silently diverge). New pin.
- **flow: `GovernorManager.get_resource_status` tolerates sync `get_limit`**
  — mirrors the stream manager's `isawaitable` handling. New pin.

### Fixed (hygiene / tooling)

- **scripts/gates: business-neutrality gate double-scan removed** — a
  duplicated scan block made every finding/advisory appear twice. New meta
  test (`tests/meta/test_gate_no_duplicates.py`).
- **protocol: `generate_schemas.py` import order** — the script's own
  sys.path injection now precedes every orditect import, so it runs on a
  bare interpreter. New meta test (`tests/meta/test_schema_generator.py`).
- **stream tests: `integration/__init__.py` no longer holds dead test cases**
  (pytest never collects them there).
- **flow/core: shielded release tasks are strong-referenced** — an orphaned
  shield task is never GC-collected mid-release (GovernedClient,
  GovernedCallClient, SemaphoreHold, @limited), mirroring the executor's
  `_finalize_tasks` discipline. New pins.

### Changed (bounded memory)

- **flow: `ActionDispatcher` dedup window is bounded** (`dedup_capacity`,
  default 10000) — an unbounded `_seen` set leaked memory over the process
  lifetime. Dedup is guaranteed within the window; beyond it a repeated
  action_id re-executes (documented semantics). New pins.
- **adapter-ui: `MemoryActionQueue` receipts are bounded** (`max_receipts`,
  default 10000) with LRU eviction. New pins.

### Changed (docs)

- `adapter-ui` queue: receipt retention window semantics documented.
- `flow/docs/governance.md`: `rebuild_dep_counters` `skipped_children`
  signals cold/hot data inconsistency — manual review and re-run required.
- stream: `TaskflowResultStore` documents `manifest` as a reserved status
  word for its task records.

### Meta / verification

- New `tests/meta/` suite: internal dependency floors match package
  versions (prevents the 0.1.5 floor/metadata drift), schema generator runs
  bare, gate has no double-scan.
- All CI gates green; traceability closure OK; schema artifacts up to date.

### Pinning-flip ledger (v0.1.6)

No assertion flips — all pinning tests are additive (previously un-pinned
paths). The flip ledger for this release is empty.
```

### 各包 CHANGELOG 补一段 `[0.1.6]`(root CHANGELOG 已涵盖细节，各包只写本包条目）

`packages/flow/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- F3 result-reuse ordering: reuse now runs before acquire/running-write
  (no wasted semaphore slot, no zombie RUNNING hot record).
- `scan_dependency_cycles` is iterative (no RecursionError on deep chains).
- `terminate` honors `request_cancel` and not-found races (returns False).
- `charge()` surfaces rejected quota writes explicitly.
- `GovernorManager.get_resource_status` tolerates sync `get_limit`.
- Shielded release tasks are strong-referenced (GovernedClient,
  GovernedCallClient).
- `ActionDispatcher` dedup window is bounded (`dedup_capacity`).

### Changed
- `docs/governance.md`: rebuild `skipped_children` manual-review guidance.
```

`packages/core/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- `task_reopen.lua` clears `progress` / `cancel_outcome` (generation-scoped
  settle metadata must not leak into a new generation); `lua_contract.md`
  updated. New pinning test.
- Shielded release tasks in `SemaphoreHold` / `@limited` are
  strong-referenced.
```

`packages/stream/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- `StreamResult` deduplicated (runner/stream.py no longer shadows the
  canonical dataclass).
- `TaskflowResultStore` documents `manifest` as a reserved status word.

### Changed
- Dependency floors raised to >=0.1.6.
```

`packages/protocol/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- `scripts/generate_schemas.py`: import order — the script's own sys.path
  injection now precedes orditect imports (runs on a bare interpreter).
- scripts/gates: business-neutrality gate double-scan removed.
```

`packages/adapter-ui/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- Query kwargs are strict (unknown kwargs raise TypeError).
- Mixed datetime forms normalized (seed objects vs file ISO strings).

### Changed
- `MemoryActionQueue` receipts are bounded (`max_receipts`, LRU eviction);
  retention window semantics documented.
```

`packages/adapter-memory/CHANGELOG.md` / `packages/adapter-local/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Changed
- Version alignment with ecosystem (no behavior change).
```

`packages/bridge-openai/CHANGELOG.md` 顶部插入：

```markdown
## [0.1.6] - TBD

### Fixed
- `_latency_ms` fully removed from the streaming path (C5 completion);
  result holder handed to cost_fn carries only endpoint vocabulary.

### Changed
- Dependency floors raised to >=0.1.6.
```

### `flow/docs/governance.md` — `rebuild_dep_counters` 一节补一句

在 `## Offline tools` 的 `stats` 说明处追加：

```markdown
`skipped_children` lists children whose rebuild was abandoned because a
parent hot record was missing — this signals cold/hot data inconsistency.
These children will NOT become ready on their own; review the data and
re-run the rebuild after reconciling, rather than relying on the counters.
```

### `stream/src/orditect/stream/adapters/taskflow.py` — `TaskflowResultStore` docstring 补一句

在其类 docstring 末尾追加：

```
    Reserved status word: the task record's status is set to the reserved
    word "manifest" (not part of any business state machine). Callers must
    never drive status transitions on these records — only the manifest
    field is updated.

## [0.1.5] - TBD

**v0.1.5 = bugfix + certification-hardening release.** No new capabilities —
every change is a fulfillment of an existing contract promise, or a
completion of the certification/documentation layer.

### Fixed (contract fulfillments)

- **snapshot merge semantics adjudicated and unified** (T3 Revision 0.1.5):
  an empty status string is the absence of status intent — never a
  mutation, never a regression; `created_at` is the first write's instant;
  `updated_at` the latest. New shared primitives
  `mechanism.fold_snapshot_rows` + `SNAPSHOT_MERGE_FIELDS`; the memory
  guard, local `_fold`, adapter-ui `_fold`, and DR-SNP-001/002 are all
  unified to it. New CF-SNP-014 pins the semantics for every adapter.
- **core: `task_reopen.lua` clears the old generation's `result`/`error`** —
  a new generation must not inherit the previous generation's output
  (prevents a crash between reopen and re-execution from being misjudged
  as REUSE by resume).
- **adapters: sorting by `expire_at` fixed** — memory raised TypeError on
  mixed None/datetime; local misordered via the string "None". Adjudicated:
  no-expiry sorts as infinitely far (ASC last, DESC first). New CF-SNP-015.
- **adapter-ui: full SnapshotReader contract alignment** — query returns
  latest generations only and now honors sort/page/time_range
  (out-of-whitelist sort/group_by raises InvalidQueryError); get_tree folds
  per (task_id, step); AuditView honors sort/page/time_range.
- **flow: `rebuild_dep_counters` hardened** — a child with any missing
  parent hot record is skipped as a whole (was: silently under-counted);
  threshold-reaching votes trigger cancel via injected lifecycle and are
  always reported (`cancelled` / `pending_cancel`); status vocabulary is
  caller-declared (was: hardcoded).

### Fixed (residual gaps from the v0.1.4 fix family)

- **core: `quota_release.lua`** — a partial release with pttl<=0 no longer
  leaves an eternal non-zero pending key (fallback TTL via ARGV[2]).
- **core: `quota_reserve.lua`** — the reaping pass no longer writes into a
  dead pending key (which suppressed the renewal rebuild); a dead counter
  with surviving leases is rebuilt from the leases before any quota
  decision, on both the renewal and fresh-reserve paths (over-admission
  fix).
- **core: `_sync_attached_ttl`** — adjudicated final semantics: an attached
  key that EXISTS follows the owner's TTL (or a fallback when the owner is
  gone — its data is real business state and must not be dropped); a key
  that does NOT exist is never materialized (genuine ghosts never pollute
  SCAN-based queries). (2 FLIPs)
- **flow: recovery rerun background tasks now retrieve exceptions**
  (aligned with the orchestrator's F5 discipline).

### Added (certification & observability)

- New consumer-tier whitelist cases CF-VIEW-005/006 (the consumer profile
  now actually verifies out-of-whitelist rejection).
- `GovernedCallClient` audit payloads now carry `cost_units` whenever
  cost_fn is evaluated (the documented "cost feeds observation" semantics).
- Gate hardening: the Lua time-source gate now matches on assignment (an
  instant variable assigned from ARGV) instead of proximity — zero false
  positives on the legitimate server-clock pattern; the error-surface gate's
  base-class matching is robust to attribute bases; business-neutrality
  excludes rules/ (aligned with api-surface).

### Changed (docs)

- adapter-local README: append is best-effort (torn tail rows are skipped
  by design); no fsync; scale boundary tightened.
- flow governance docs: external-vocabulary boundary of cancel/terminate;
  exemption holder-liveness boundary recorded (tracked separately).
- bridge-openai: `_latency_ms` no longer leaks into the caller-visible
  provider response (audit uses the client's `elapsed_ms`; 2 FLIPs).
- adapter-local result: `stream_id` is validated as a safe path segment.

### Pinning-flip ledger (v0.1.5)

| file | flip |
|---|---|
| packages/core/tests/pinning/test_v011_dep_primitives.py | ghost counter: deleted → materialized-with-fallback-TTL (final adjudicated semantics) |
| packages/core/tests/pinning/test_v011_dep_primitives.py | ready-scan ghost: absent → present-unfiltered / excluded-by-status-filter |
| packages/bridge-openai/tests/test_client.py | latency_ms → elapsed_ms (2 places) |

## [0.1.4] - TBD

**v0.1.4 = bugfix-only release.** No new capabilities — every change is a
fulfillment (and pinning) of an existing contract promise. Each fix below
carries a "red before / green after" pinning test.

### Fixed (contract violations)

- **flow: not-found contract unified** (FLIP). `TaskRedisDB.get_task`
  returns `{}` for missing tasks (documented storage contract); flow
  layers (`cancel` / `terminate` / `get_status` / `wait_terminal`) now
  check for emptiness and behave per contract — `cancel`/`terminate`
  return `False`, `get_status`/`get_task`/`wait_terminal` raise
  `TaskNotFoundError` (previously an undeclared `KeyError` escaped on the
  real backend; the unit suite only covered in-memory fakes whose
  `get_task` raises).
- **protocol/adapters: T4 idempotency no longer polluted by producer
  clocks** (FLIP). Idempotency comparison now excludes mechanism clock
  fields (`created_at` / `updated_at` / `registered_at`) via the new
  `mechanism.idempotent_payload_equal` primitive; a retry reconstructing
  identical business content with a new producer timestamp is a silent
  dedup, not a conflict. Applied to `save_terminal` (snapshot) and
  `append` (audit) in both adapters, and to DR-AUD-001.
- **core: `quota_reserve.lua` idempotent-renewal TTL gap**. A renewal
  chain (>= 2 renewals, each within task_ttl) could push a lease's
  logical lifetime past `pending_key`'s fallback TTL; `pending_key` dying
  while a live lease remained evaporated the lease's units from the
  counter and let a later reserve over-admit. The idempotent branch now
  bumps `pending_key`'s TTL (or rebuilds it from surviving leases when
  already dead). `lua_contract.md` updated.
- **core: `quota_release.lua` eternal "0" residue**. Releasing to zero
  with no TTL now `DEL`s `pending_key` instead of leaving an eternal
  `"0"` key.
- **core: ghost dependency counter keys get a TTL fallback**.
  `_sync_attached_ttl` falls back to `default_expire_time` when the owner
  hot record is already gone, so a counter created by DECR on a missing
  key is never eternal.
- **adapters: T3 second-face merge semantics unified**. A sparse
  same-generation save no longer erases previously recorded non-state
  fields (parent_task_id, pointers, error, cost, model, expire_at);
  status advances only with a non-empty incoming value. New CF-SNP-013
  pins this; `domains/snapshot.py` merge rule documented.
- **flow: business hooks can no longer crash finalization**. Hook calls
  (`on_success` / `on_failure` / `on_cancel`) are wrapped in try/except
  (T9), and shielded finalize tasks now retrieve exceptions (no more
  "exception was never retrieved" warnings).
- **stream: `cancel()` never blocks on a full mux queue**. The cancelled
  event is best-effort (state is authoritative in the token); the control
  path no longer waits on the data path.
- **flow: `GovernedCallClient.call_streaming` evaluates `cost_fn`
  regardless of budget presence** (its output feeds both budget charging
  and observation; it was incorrectly gated behind the budget branch).
- **flow: budget-blocked `GovernedCallClient.call` no longer audited**.
  The budget pre-check now happens before the audited region, so a
  blocked attempt (which never acquired a resource) leaves no record.

### Fixed (test/CI infrastructure)

- **conformance: CF-SNP-011/012 registered under the wrong half-domain**
  (`audit_sink` instead of `snapshot_sink`) — they were skipped on every
  adapter, so sort/group_by whitelist verification never actually ran.
  Moved to `cases_snapshot.py`; new meta-pinning test
  (`TestCaseRegistrationIntegrity`) prevents recurrence.
- **stream: `test_stream_break_marks_cancelled_and_pointerizes_partial`
  raced the generator's finally block** — the audit write lands one
  event-loop tick after `break`; the test now yields before asserting.
- **bridge-openai: streaming tests now close the generator explicitly**
  so the finally chain (charge + audit) executes deterministically;
  `result_fn` returns None only when usage is actually absent (A5).
- **adapter-ui: `SnapshotView.aggregate` folds to the latest generation
  per node** (CF-VIEW-004 semantics); `test_aggregate` flipped
  accordingly. `test_idempotent_action_dedup` flipped: the second pause
  on an already-terminal task is now correctly REJECTED.

### Changed (docs & hygiene)

- `flow/protocols/storage.py`: `get_task` contract documented ("missing
  task returns `{}`; callers must check emptiness").
- `stream/client/resolver.py`: status words are now caller-injectable
  (`success_words` / `terminal_words`), defaulting to the flow vocabulary
  (T6; was hardcoded).
- `core/docs/lua_contract.md`: rewritten in English; preserve-mode TTL
  precision boundary documented (integer-second rounding; index member
  may be lazily cleaned up to <1s before the primary record); legacy
  version references consolidated.
- `core/docs/design_decisions.md`: rewritten for orditect (legacy
  taskbase handover content removed; DD-001..DD-010 retained with
  rationale).
- `orchestrator`: `dependency_governor` is documented as attach-only
  (callers must wire `notify_task_terminal` themselves).
- Repo hygiene: removed committed `build/` trees, stale `*.egg-info`
  (0.1.2), and the mistakenly packaged `packages/stream/src/orditect/
  stream/tests/` directory; `.gitignore` updated (`build/`, `*.egg-info/`,
  `vocabulary-advisory.txt`); deduplicated a `CapabilitySet` import;
  fixed `_is_match`'s docstring; removed dead `default_task_data
  ["timestamp"]` value and dead test code.

### Verification
- All CI gates green; traceability closure OK (12 terms / 29 CF cases /
  9 DR rules).
- Every fix carries a red-before/green-after pinning test (see FLIP
  ledger).

## [0.1.3] - TBD

**v0.1.3 = three-category protocolization.** Core / adapters / bridges
communicate through protocols, not implementations. Reference
implementations prove the protocols work; swap tests prove they're
interchangeable.

### Added
- **GovernedCallClient** (flow): standard form of one governed call
  (GovernedClient + observation + opaque labels).
- **call_id dual-habitat idempotency**: quota hot path and audit cold path
  share the same call_id.
- **ActionDispatcher** (flow): asynchronous action executor (command-queue
  form, DD-013).
- **orditect-adapter-local**: local-file storage adapter (document-family
  reference, full profile).
- **orditect-adapter-ui**: UI adapter reference (consumer read + action
  sink).
- **orditect-bridge-openai**: OpenAI-compatible endpoint bridge (producer
  tier reference).
- **check_import_boundary.py** gate: package dependency enforcement.

### Changed
- Action sink form: direct invocation → command-queue (DD-013 bypass).

### Verification
- adapter-local: full profile ✅
- bridge-openai: producer profile ✅
- adapter-ui: consumer + action profiles ✅
- End-to-end governance loop verified ✅
- Swap tests: adapters/bridges interchangeable ✅
- All CI gates green ✅

### Freeze criteria progress
- Document backend: ✅ (adapter-local)
- External producer: ✅ (bridge-openai)
- UI interaction: ✅ (adapter-ui)
- Relational backend: ❌ (PG, commercial layer)

### Documentation
- `docs/integration-guide.md`: three-category integration guide with
  certification checklist and boundary discipline.

### Explicitly Not Done
- No concrete adapter/bridge productization (reference implementations only).
- No new governance mechanisms (all reuse existing core/flow assets).
- No business concepts in framework layers (OpenAI vocabulary stays in bridge).
- No UI frontend implementation (that's product layer).
- No apps / gateways / knowledge packs / clients / SDKs (outside framework).
- No framework graph-structure mapping spec (deferred to optional layer 2).
- No SaaS / hosted services.

## [0.1.2] - 2026-08-28

**v0.1.2 = the format-standardization release.** Mechanisms were proven in
v0.1.0/v0.1.1; this release extracts what the framework already does right
into publishable, verifiable, implementation-independent criteria.

### Milestones
- M0: gate-system replacement (principle gates retire the diff-based freeze)
- M1: wire-format artifacts + protocol model hardening + conformance
      single-loop rebuild + 12 new CF cases
- M2: dependency graph becomes the fifth contract domain (T12)
- M3: conformance tiers (full / producer / consumer) + bridge discipline
- M4: data-rule toolkit (9 DR rules + reference executor) + three-way
      traceability + three static gates
- M5: term evolution policy; T7/T10 Revision 0.1.2; contracts.md;
      wire-format.md final; version alignment

### Stability declaration
The v0.1 format is **ratifying, not frozen**. The 1.0 freeze requires:
one relational-family backend (PG or SQLite) passing the full profile,
one document-family backend (localfile) passing the full profile, and one
external producer (a bridge) passing the producer profile.

### Pinning-flip ledger
See `flip-ledger-0.1.2.md` (2 flips, both from the WI-1.4
AuditEvent.created_at unification).


## [0.1.2] - TBD (M4: data rules + static gates)

### Added
- **Data-rule toolkit** (`orditect.protocol.rules`): 9 machine-checkable
  invariants over serialized data (6 violation-level, 3 warning-level),
  with a library-level reference executor (`run_rules`). Data-level
  verification complements the API-level conformance suite — any producer
  can self-certify its output without running an adapter.
- `docs/data-rules.md`: rule specs, violation/warning level criterion,
  dangling_pointers exemption channel, verification discipline.
- **Three new static gates** (stdlib-only):
  - `check_schema_vocabulary.py` — publishable schema artifacts are
    vocabulary-neutral (fields/enums/keywords);
  - `check_lua_time_source.py` — clock readings in Lua come from
    redis.call('TIME'), never from ARGV (instants vs durations);
  - `check_error_surface.py` — contract methods raise only ContractError
    subclasses (T9).
- Traceability upgraded to a three-way closure: terms <-> CF cases <-> DR
  rules.

### Changed
- **Lua transition window closed**: the server-side-clock criterion of the
  Lua modification policy (temporarily enforced by two-person review since
  M0) is now enforced by `check_lua_time_source.py` in CI.

## [0.1.2] - TBD (M0: gate-system replacement)

### Changed
- **Retired the v0.1.1 diff-based freeze gate** (`scripts/check_v011_frozen.py`).
  Rationale: v0.1.1's gate protected assets that v0.1.2 legitimately evolves
  (the protocol package and, under policy, the Lua scripts). Protection is
  upgraded from "these directories must not change" to "these principles must
  not break": three stdlib-only CI gates (business-neutrality, import-boundary,
  api-surface) plus the four-part Lua modification policy.
- Transition window: until the Lua time-source gate lands (M4), the
  server-side-clock criterion of the Lua policy is enforced by two-person PR
  review instead of a script. The window closes when that gate ships.

### Added
- `scripts/gates/` — principle-based CI gates (stdlib-only, no pip install):
  business-neutrality (G1/G2/G3 positions + advisory report),
  import-boundary (business/internal/third-party classification),
  api-surface (active-verb + scheduling-field scan).
- `scripts/gates/list_pin_flips.py` — pinning-flip ledger generator.
- CONTRIBUTING.md — gate discipline + FLIP marker discipline.