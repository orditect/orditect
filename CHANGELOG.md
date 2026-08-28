# Changelog

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