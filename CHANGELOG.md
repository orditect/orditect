
# Changelog

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