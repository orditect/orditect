# Contributing

## Concurrency-primitive change discipline

Changes to concurrency primitives (heartbeat scheduling, backpressure,
cancellation cascades, shielded release) follow the same two-person review
discipline as the Lua transition window: single-reviewer blind spots in
this area are systemic (v0.1.7 SSE heartbeat postmortem: four independent
defects hid behind one symptom).

Before writing the fix, the PR description must answer the structural
question: **who blocks whom during the failure window?** A fix that
schedules work on top of the very wait that is blocked during the failure
cannot work (the pre-fix heartbeat awaited the runner's next event, which
is exactly what never arrives during a quiet period).

## Pinning-test authoring principles

- Pin the seam, not the coincidence: assert at the mechanism boundary
  (frame bytes, merged-queue output, fold results), never at
  timing-dependent intermediate states (backpressure windows, gated emits,
  scheduler races).
- Verify atomic facts before assembling scenarios: when a pin fails
  repeatedly, reduce the scenario to the smallest observable fact (what do
  the frame bytes actually look like? what type does this API actually
  accept?). If the reduced fact is wrong, the implementation is wrong —
  stop adjusting the test.
- A pin failing in setup is still doing its job: the v0.1.7 pins found
  four latent defects by failing for the "wrong" reason before the
  "right" reason could even be reached.

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