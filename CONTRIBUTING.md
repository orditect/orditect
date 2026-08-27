# Contributing

## Gate discipline (v0.1.2+)

v0.1.1's diff-based freeze gate (`scripts/check_v011_frozen.py`) is retired.
Protection is now principle-based, enforced by CI gates in `scripts/gates/`:

- `check_business_neutrality.py` — no business vocabulary on the protocol
  contract surface (data criterion)
- `check_import_boundary.py` — package dependency direction + no business
  package imports (dependency criterion)
- `check_api_surface.py` — no orchestration verbs on the contract surface
  (behavior criterion)

The gates are stdlib-only: they must never require pip-installing anything.

### Lua scripts (transition window)

The v0.1.1 freeze on `packages/core/src/orditect/core/lua/` is lifted.
Lua scripts may evolve under the four-part policy (see `docs/` planning):
no business fields, no atomicity downgrade, `lua_contract.md` + pinning
updated in the same commit, server-side clock only. Until the Lua
time-source gate lands (M4), every Lua change requires two-person PR review.

## Pinning-flip discipline (FLIP markers)

When a change intentionally flips the assertion of a pinning / golden /
conformance test, the flip MUST be marked at the flipped line (same or
previous line) with:

    # FLIP(vX.Y.Z): <reason> — <source: WI id or doc section>

Rules:
- Code change and assertion flip land in the SAME commit.
- Every marker carries a version and a reason pointing at its decision source.
- `python scripts/gates/list_pin_flips.py` generates the flip ledger for the
  CHANGELOG; unmarked flips are review failures.