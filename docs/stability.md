# Orditect Stability Commitments (v0.1.x)

**Audience**: anyone building on Orditect for more than a demo. This page
states, in one place, what you may rely on across releases and what may
still evolve. Where this page and a package CHANGELOG disagree, the
CHANGELOG wins for that release and this page must be fixed.

**Versioning**: packages share one ecosystem version line (currently
0.1.x). Semantic versioning applies per package; a commitment below is
honored from the version it was frozen.

## Commitment levels

| Level | Meaning |
|---|---|
| **frozen** | Never changes within the level's scope. Breaking it is a bug. |
| **stable** | Changes only with a minor/major bump AND a CHANGELOG entry. |
| **ratifying** | Being ratified; may be corrected by second-backend / producer feedback before 1.0. Corrections land with a CHANGELOG entry and (for behavioral ones) a FLIP marker. |
| **internal** | May change in any release without notice. Underscore-prefixed names and anything not listed below are internal by default. |

## Frozen

| Surface | Since | Scope of the guarantee |
|---|---|---|
| Trace-bundle row envelope `{"v","op","ts","data"}` | v0.1 | Field names and their meaning. `op` values may be **appended only**; existing values (`save` / `save_terminal` / `append` / `edge_write` / `put`) are never renamed or removed. |
| Execution identity model (T11) | v0.1 | `execution_id` semantics: one concept, three projections (core hot record / flow execution / protocol snapshot), assigned at creation, advanced only by reopen. |
| Trace-bundle file layout | v0.1.3 | `snapshots.ndjson` / `audit.ndjson` / `deps.ndjson` / `results/<id>.json` / `content/sha256/<aa>/<digest>` under the bundle root. New files may be added; existing ones keep their role. |

## Stable

| Surface | Notes |
|---|---|
| Lua script KEYS/ARGV call specifications | The 10 scripts' signatures change only per `packages/core/docs/lua_contract.md`, with the doc updated in the same commit. Script *bodies* may change under the four-part policy (no business fields, no atomicity downgrade, server-side clock, doc + pinning in the same commit). |
| The 12 normative terms (T1–T12) | Evolution policy in `packages/protocol/docs/terms.md`: append-first, tighten-only, tombstone on retirement. |
| Public injection points | `snapshot_sink` / `snapshot_query`, `task_factory`, governor / quota duck types (`acquire`/`try_acquire`/`release`/`get_usage`; `reserve_units`/`get_pending_units`), `governor`-in-`kwargs` injection, sink protocols (`write` / `append` / `save` / `write_dependency`), `EnricherProtocol`, `LLMSourceProtocol`. New points may be added; existing signatures are stable. |
| SSE event protocol | Event types append-only; `stream.end` is the only terminal signal; envelope has exactly the six documented top-level fields. Golden-test locked. |
| Conformance profiles | `full` / `producer` / `consumer` tiers and their eligibility rules. |

## Ratifying

| Surface | Why it is not yet stable |
|---|---|
| JSON Schema artifacts (`packages/protocol/schemas/`) | Await feedback from a second real backend (the protocol 1.0 freeze criterion). |
| op-envelope vocabulary beyond the frozen set; `edge_write` op; URN convention | Marked experimental in `docs/wire-format.md`. |
| Example code (`examples/*`) | Examples are teaching material, tested for green runs, not a compatibility surface. They may be restructured between releases. |
| `MemoryActionQueue` / in-memory doubles | Reference implementations (single-process, bounded retention). Production deployments are expected to supply their own queue/governor behind the same duck types. |

## Internal (no compatibility promise)

- Any underscore-prefixed module, class, method, or attribute.
- Redis key layouts and index structures (key prefixes are configurable
  for isolation, but the layout itself is internal).
- `GovernedLLMClient` private composition (`_call`, `_http`, `_payload`,
  `_audit_payload`). Use its public `chat()` / `stream()` only.
- Log message texts and levels (they are diagnostics, not an API).

## Compatibility floor rule

Every package's internal dependency floor equals its own version
(`orditect-x>=<same version>`). Mixing versions inside the 0.1 line is
not supported: upgrade the whole set together.