# Conformance profiles: three compliance tiers

The conformance suite certifies implementations against the contract.
Because implementations come in three distinct shapes, certification comes
in three tiers. **A profile is a minimum bar, not a ceiling: every
half-domain an implementation declares is verified, regardless of profile
(term T8).**

## The three tiers

| Profile | For | Requirement | Verification |
|---|---|---|---|
| `full` | Storage backends | sink/query declared **in pairs** (eligibility) | All sink cases + per-case query guards |
| `producer` | Bridges (external frameworks) | sink half-domains as declared | Sink cases only; query not required |
| `consumer` | Visualization / diagnostics | query half-domains as declared | Query guards + seeded CF-VIEW cases |

One-line semantics: **producer = writes correctly; consumer = reads
correctly; full = does both, in pairs.**

## Usage

```python
from orditect.protocol.conformance import run_conformance

# storage backend (must declare sink/query in pairs)
report = run_conformance(my_store.snapshot, profile="full")

# bridge (e.g. an external-framework exporter): two sinks are enough
report = run_conformance(my_bridge, profile="producer")

# read-only tool: needs seed() for the deep read cases
report = run_conformance(my_viewer, profile="consumer")

assert report.failed == 0, report.summary()
assert report.eligibility_error is None
```

## Eligibility (full tier)

Declaring `x_sink` without `x_query` (or vice versa) under `profile="full"`
makes the whole tier **ineligible**: the report carries `eligibility_error`
and no case results. An eligibility problem is not a case failure —
`report.failed` stays 0; check `report.eligibility_error` first.

## Consumer tier and the seed hook

CF-VIEW cases verify deep read semantics (trees, expiry, graph closures,
aggregation) over **pre-seeded** data. A consumer adapter opts in by
implementing the extra-contract hook:

```python
async def seed(self, fixtures: dict) -> None:
    """Consume fixtures from conformance.fixtures.consumer_fixtures().

    fixtures = {"snapshots": [payload dicts], "edges": [payload dicts]}.
    Datetime fields arrive as ISO strings (convert with fromisoformat).
    MUST be idempotent (T4): seed may be invoked repeatedly.
    """
```


Without `seed`, CF-VIEW cases degrade to **skipped** with the note "seed not
implemented; consumer verification limited" — a documented degradation, not
a failure.

The fixtures' dict shapes are the Python expression of the future YAML test
vectors; the M4 data-rule toolkit shares this data lineage.

## Report semantics

`ConformanceReport.summary()` prints the tier plus a half-domain coverage
line. Three result layers must not be confused:

| Layer | Field | Meaning |
|---|---|---|
| eligibility | `eligibility_error` | the tier's hard requirement is unmet |
| case failure | `failed > 0` | a contract violation was detected |
| skip | `skipped` | capability not declared (T8) or seed absent |

## Case ownership

A case belongs to the profile tier of its **write half-domain**; read-side
verification is guarded per-case (`supports("..._query")`). CF-VIEW cases
are consumer-only and registered under the suite-internal `view` pseudo
half-domain — never a CapabilitySet flag.

---


