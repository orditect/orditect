# Lua Script Call Contract (orditect v0.1.4)

This document freezes the KEYS/ARGV specifications and return formats of all
Lua scripts in orditect-core. Upper frameworks (orditect-flow / orditect-stream
/ future frameworks) MUST pass arguments per this contract. Any spec change
requires a version bump and a synchronous update of this document in the same
commit.

All scripts read the server-side clock via `redis.call('TIME')` — clock
readings (instants) never come from script arguments. Durations (TTL seconds,
lease lengths, refill frequencies) are caller-declared and legitimately arrive
via ARGV.

## Index lease model (design contract)

Shared indexes (status index / lineage index) use a ZSET lease model isomorphic
to the semaphore/quota primitives:

- **Member-level precise semantics**: member = task_id, score = expire_at_ms
  (computed from the server-side clock).
- **Lazy read-path cleanup**: readers first `ZREMRANGEBYSCORE key -inf now_ms`,
  then take members. The key auto-evaporates when its members are drained
  (inherent aggregate-type behavior); there is no separate cleanup mechanism.
- **Key-level TTL only ever increases**: on write, `TTL < expiry or no TTL ->
  EXPIRE expiry`. This is a residue fallback for "never read again" scenarios
  only and carries no precise semantics.
- This model fixes the flaw of key-level shared TTL: with a shared collection
  whose members have independent lifetimes, no single key TTL can satisfy both
  early and late writers (active members dying by contagion / ghost members
  accumulating).

## task_init.lua — atomic task initialization

**KEYS**:
- KEYS[1]: task_key (primary task record key)
- KEYS[2]: status_index_key (pass KEYS[1] as a placeholder when ARGV[5]="0";
  the script does not write it)
- KEYS[3]: children_index_key (pass KEYS[1] as a placeholder when ARGV[6]="";
  the script does not write it)

**ARGV**:
- ARGV[1]: data_json (initial task record, JSON object)
- ARGV[2]: expiry_seconds
- ARGV[3]: task_id
- ARGV[4]: if_not_exists ("1"/"0")
- ARGV[5]: has_status ("1"/"0"; "0" when initial_status is the empty string)
- ARGV[6]: parent_task_id ("" = no lineage registration)
- ARGV[7]: execution_id (initial generation identity, generated Python-side as
  `exec-{uuid4hex[:12]}`). A task carries a generation identity from creation
  (T11 hot-path projection); reopen_task only advances the generation. An
  idempotent skip does not rewrite the existing execution_id.

**Returns**:
- 1: initialization succeeded
- 0: idempotent skip (if_not_exists="1" and the task already exists)

**Semantics**:
- The existence check and the write are atomic in one script (EXISTS +
  conditional write), sealing the TOCTOU window.
- Status / lineage indexes are written per the lease model (ZADD expire_at +
  key-level TTL increase-only).
- Placeholder-key discipline: when an index must not be written, KEYS[2]/KEYS[3]
  receive KEYS[1]; the script is gated by ARGV[5]/ARGV[6] and never writes the
  placeholder key.

## task_update.lua — atomic task record merge + status index maintenance

**KEYS**:
- KEYS[1]: task_key (primary task record key)

**ARGV**:
- ARGV[1]: updates_json (fields to merge, JSON object)
- ARGV[2]: expiry_seconds. **A negative value means "preserve the remaining
  expiry instant"** — the script reads the primary record's TTL (falling back
  to ARGV[7] when no TTL exists); >= 0 advances the expiry explicitly.
- ARGV[3]: status_index_prefix (status index key prefix)
- ARGV[4]: validate_transfer ("1"/"0"; reserved, not consumed by the script —
  full state-machine validation lives Python-side)
- ARGV[5]: task_id
- ARGV[6]: terminal_statuses_json (caller-declared terminal status set)
- ARGV[7]: default_expiry_seconds (fallback for preserve mode when the record
  has no TTL)

**Returns** (cjson):
- `{"ok": true}` success
- `{"ok": false, "err": "NOT_FOUND"}` task does not exist
- `{"ok": false, "err": "INVALID_TRANSFER"}` a terminal state was overwritten
  (old_status is within the terminal set declared by ARGV[6])

**Semantics**:
- Terminal protection executes unconditionally (whitelist: only words declared
  in ARGV[6] are protected).
- Status index maintenance (lease model):
  - On status change: ZREM from the old index + ZADD to the new index
    (score = expire_at).
  - On unchanged status: ZADD as well (the primary record's EX was reset, so
    the lease advances in step; ZADD is idempotent for existing members).
  - The new index key's TTL only ever increases.
- Precision boundary (documentation only): preserve mode reads `TTL` as integer
  seconds (rounded down), so high-frequency updates lose a sub-second fraction
  per call; an index member's `expire_at` shares the same rounding and may be
  lazily cleaned up to <1s before the primary record — `list_task_ids_by_status`
  may briefly miss a task during the primary record's final second.

## task_reopen.lua — reopen a terminal task as a new generation

**Position**: the hot-path primitive of the recovery plane. It provides a
"controlled new generation" for breakpoint-resume and mid-point rerun — it is
NOT a state transition and does not violate terminal protection (T3): terminal
protection holds unconditionally within one execution generation; reopen only
produces a new one.

**KEYS**:
- KEYS[1]: task_key (primary task record key)

**ARGV**:
- ARGV[1]: task_id
- ARGV[2]: new_execution_id (generated Python-side, format `exec-{uuid4hex[:12]}`)
- ARGV[3]: initial_status (post-reopen initial status word, caller vocabulary)
- ARGV[4]: expiry_seconds (new generation lease; <0 = keep the remaining TTL,
  same preserve semantics as task_update.lua; ARGV[7] is the no-TTL fallback)
- ARGV[5]: status_index_prefix (status index key prefix)
- ARGV[6]: terminal_statuses_json (caller-declared terminal status set)
- ARGV[7]: default_expiry_seconds (preserve-mode fallback)

**Returns** (cjson):
- `{"ok": true, "execution_id": "...", "previous_status": "..."}` success
- `{"ok": false, "err": "NOT_FOUND"}` task does not exist
- `{"ok": false, "err": "NOT_TERMINAL", "current_status": "..."}` the current
  status is not within the terminal set declared by ARGV[6] — explicit rejection

**Semantics** (atomic in a single script):
1. Read the primary record; missing -> NOT_FOUND.
2. Read the current status; not in the terminal set -> NOT_TERMINAL (the
   terminal vocabulary is entirely caller-injected; the script embeds no words
   — vocabulary neutrality, T6).
3. Atomic write:
   - append the old execution_id to `previous_execution_ids` (skipped when no
     old id exists; the array is an audit trail capped at 50, oldest dropped —
     prevents hot-record bloat);
   - overwrite `execution_id` with ARGV[2];
   - record `previous_status` (for audit/observation);
   - reset `status` to ARGV[3];
   - reset `cancel_requested` to false (the new generation's cancel flag);
   - clear `result`, `error`, `progress` and `cancel_outcome` — a new
     generation must not inherit the previous generation's output or
     settle metadata (v0.1.5 / v0.1.6);
   - write `reopened_at` from the server clock;
   - set the primary record's EX per ARGV[4] semantics (explicit / preserve /
     default fallback).
4. Status index migration (ZSET lease model, isomorphic to task_init/task_update):
   - ZREM from the old status index;
   - ZADD to the new initial-status index (score = server now_ms + expiry×1000);
   - the new index key's TTL only ever increases (residue fallback).
5. The lineage index is untouched (parent-child relations persist across
   generations — a child task's own reopen does not change its parent's
   registration).

**Concurrency semantics (T4/T10)**: concurrent reopen of the same terminal task
executes atomically in the single script — the first caller completes the state
reset (new-generation initial status); later callers read the initial status
(non-terminal) and are rejected with NOT_TERMINAL. Exactly one winner; no
double new generation.

**Relation to sibling primitives**:
- `initialize_task(if_not_exists=True)` = skip when present (submit-side
  dedup);
- `update_task` = state transition within one generation (terminal protection
  unconditional);
- `reopen_task` = open a new generation after terminal (recovery/replay).
The three are complementary, covering the three orthogonal actions of a task
lifecycle.

## sem_acquire.lua — ZSET lease semaphore acquire

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: limit (concurrency cap)
- ARGV[2]: lease_time (lease duration, seconds)
- ARGV[3]: token (token value)

**Returns**:
- token (success)
- nil (full)

**Semantics**:
- Reap expired occupancies (score < now - lease_ms).
- ZADD when ZCARD < limit, and set the key TTL to lease × 2
  (`math.max(1, math.floor(lease × 2))` — integerized so fractional leases such
  as 0.7 do not raise `ERR value is not an integer`).

## sem_refresh.lua — watchdog renewal

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: token
- ARGV[2]: lease_time_sec (also refreshes the key-level TTL)

**Returns**:
- 1 (renewed)
- 0 (token no longer exists; stop renewing)

**Semantics**:
- When the token exists, ZADD to refresh its score AND EXPIRE to refresh the
  key TTL (lease × 2, integerized). Refreshing only the score would let the
  whole key evaporate after 2×lease while long-held slots block new acquires
  from refreshing the TTL — the false-mutual-exclusion relapse.

## sem_release.lua — idempotent release

**KEYS**:
- KEYS[1]: semaphore key

**ARGV**:
- ARGV[1]: token

**Returns**: 1 (always succeeds; idempotent)

## bucket_acquire.lua — reservation token bucket

**KEYS**:
- KEYS[1]: bucket key

**ARGV**:
- ARGV[1]: capacity (bucket capacity)
- ARGV[2]: refill_amount (tokens per refill)
- ARGV[3]: refill_frequency (refill interval, seconds)
- ARGV[4]: max_sleep_ms (maximum allowed wait in milliseconds; a huge value
  means infinite wait)

**Returns**: `{status, slot_ms, server_now_ms}`
- status=1: reservation succeeded, slot_ms = reserved slot timestamp (ms)
- status=0: rejected (estimated wait exceeds max_sleep); no state committed
- server_now_ms: the server-side clock at script execution (ms). The client
  computes the wait as `wait_ms = slot_ms - server_now_ms` — the same clock
  source as the script, rejecting client-clock drift.

**Semantics**:
- Server-side clock (`redis.call('TIME')`); client clocks never pollute shared
  state.
- State TTL is self-computed (furthest reserved slot expiry + time to refill
  to full capacity + margin), never hardcoded — a slow bucket's state must not
  evaporate early and silently reset to full capacity (which would re-issue
  already-reserved slots).

## quota_reserve.lua — quota reservation

**KEYS**:
- KEYS[1]: pending_key
- KEYS[2]: leases_key (ZSET)

**ARGV**:
- ARGV[1]: units
- ARGV[2]: max_units
- ARGV[3]: task_ttl_sec
- ARGV[4]: task_id

**Returns** (cjson):
- `{"ok": true, "reason": "", "current": N, "reserved": M}` success
- `{"ok": true, "reason": "already_reserved", ...}` idempotent hit
- `{"ok": false, "reason": "limit_exceeded", ...}` over limit
- `{"ok": false, "reason": "invalid_units"/"invalid_max_units", ...}` bad args

**Semantics**:
- Reap expired leases (ZSET score < now - ttl_ms) before any decision; the
  reaping pass only touches `pending_key` when it exists (writing into a
  dead key would suppress the rebuild logic below).
- When `pending_key` is dead but leases survive, the counter is rebuilt
  from the surviving leases before the quota check (the reaping pass has
  already run, so the surviving leases are the single source of truth);
  this applies to both the renewal and the fresh-reserve paths.
- `units=0` is legal (ledger-open semantics: register a lease slot without
  consuming quota; consumed by flow's BudgetLedger.open()). `units<0` returns
  invalid_units.
- `pending_key` TTL fallback: every write bumps it (EXPIRE to leases_ttl when
  missing or shorter), preventing residue after a scope is abandoned.
  `leases_ttl = math.max(1, math.floor(task_ttl × 2))` (integerized).
- Idempotent hit (already_reserved): refreshes the lease score and key TTLs —
  a retry IS a renewal, preventing a long-running retried task from being
  reaped as crashed.
- **The renewal must also keep `pending_key` alive for at least as long as the
  renewed lease**. A renewal chain (>= 2 renewals, each within task_ttl of the
  previous) can push the lease's logical lifetime (score + task_ttl) beyond
  `pending_key`'s fallback TTL (set at first reserve). If `pending_key` dies
  while a live lease remains, the lease's units evaporate from the counter and
  a later reserve over-admits. Therefore, in the idempotent branch: when
  `pending_key` exists, bump its TTL; when it is already dead, rebuild it from
  the surviving leases (ZRANGE + HGET sum — the reaping pass above already
  ran, so ZRANGE yields exactly the un-expired leases, the single source of
  truth) and set `EX leases_ttl`.
- Idempotent hit (already_reserved): refreshes the lease score and key TTLs —
  a retry IS a renewal, preventing a long-running retried task from being
  reaped as crashed.
- **The renewal must also keep `pending_key` alive for at least as long as the
  renewed lease**. A renewal chain (>= 2 renewals, each within task_ttl of the
  previous) can push the lease's logical lifetime (score + task_ttl) beyond
  `pending_key`'s fallback TTL (set at first reserve). If `pending_key` dies
  while a live lease remains, the lease's units evaporate from the counter and
  a later reserve over-admits. Therefore, in the idempotent branch: when
  `pending_key` exists, bump its TTL; when it is already dead, rebuild it from
  the surviving leases (ZRANGE + HGET sum — the reaping pass above already
  ran, so ZRANGE yields exactly the un-expired leases, the single source of
  truth) and set `EX leases_ttl`.
  **Reaping window semantics (precision note)**: the reaping pass expires a
  lease when `score < now_ms - ttl_ms`, where `ttl_ms` comes from THE
  CURRENT CALL's `task_ttl_sec` — not from each lease's own registration
  ttl. The window is uniform across all leases in a scope: a lease is
  considered alive iff it was (re)registered within the current caller's
  declared ttl. Callers must therefore use a consistent `task_ttl_sec`
  across all calls sharing a scope; mixing a long-ttl registration with a
  short-ttl call will reap leases the caller may have expected to survive.

## quota_release.lua — quota release

**KEYS**:
- KEYS[1]: pending_key
- KEYS[2]: leases_key

**ARGV**:
- ARGV[1]: task_id
- ARGV[2]: fallback_ttl_seconds (applied when pending_key would otherwise
  become eternal; the release side does not know the original task_ttl, so
  the caller passes a safe default — currently default_expire_time)

**Semantics**:
- SET clears any existing TTL, so the release path reads the TTL first and
  restores it, preserving `pending_key`'s fallback expiry (paired with the
  reserve path's bump).
- TTL reads integer seconds, so a key in its final second reports pttl=0;
  in that case (or when the key never had a TTL) the fallback TTL from
  ARGV[2] is applied — a partial release must never leave an eternal
  non-zero counter.
- When the counter reaches 0 and the key has no TTL, `pending_key` is
  `DEL`ed instead of being SET to `"0"` — an eternal "0" key is pure
  residue with no semantic value.



## json_merge.lua — generic atomic JSON merge

**KEYS**:
- KEYS[1]: key

**ARGV**:
- ARGV[1]: updates_json (fields to merge, JSON object)
- ARGV[2]: expiry_seconds

**Returns** (cjson):
- `{"ok": true}` success
- `{"ok": false, "err": "NOT_FOUND"}` key does not exist
- `{"ok": false, "err": "NOT_A_JSON_OBJECT"}` the stored value is not a JSON object
- `{"ok": false, "err": "BAD_UPDATES_JSON"}` the argument is not a valid JSON object

**Semantics**:
- Atomic read-modify-write (a shared primitive reused by every
  read-modify-write scenario: task records, dataset records, agent state, etc.;
  domains do not write their own specialized variants).