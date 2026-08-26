"""Conformance profiles: three compliance tiers (data table, no logic).

Semantics (docs/conformance.md is the authoritative text; this table is its
executable projection):

- full:     complete storage backend. Hard requirement: sink/query declared
            in pairs (eligibility check). Case set = all sink cases + the
            per-case query-side guards.
- producer: producer (bridge). Sink half-domains verified as declared;
            query not required (any additionally declared query half-domain
            is STILL verified — a profile is a minimum bar, not a ceiling).
- consumer: consumer (visualization / diagnostics). No sink cases; query
            verified as declared + CF-VIEW seeded cases (requires the
            adapter to implement `seed`, else degraded skip).
"""

from __future__ import annotations

SINK_DOMAINS: frozenset[str] = frozenset({
    "content_sink", "audit_sink", "result_sink",
    "snapshot_sink", "dependency_sink",
})

#: profile -> sink half-domains whose cases participate in case selection
PROFILE_SINKS: dict[str, frozenset[str]] = {
    "full": SINK_DOMAINS,
    "producer": SINK_DOMAINS,
    "consumer": frozenset(),
}

#: profile -> require sink/query pairing (full-tier eligibility check)
PROFILE_REQUIRES_PAIRING: dict[str, bool] = {
    "full": True,
    "producer": False,
    "consumer": False,
}

#: pseudo half-domain for the seeded consumer cases (never a CapabilitySet
#: flag; exists only inside the suite's case-selection logic)
VIEW_DOMAIN = "view"

PROFILES: frozenset[str] = frozenset(PROFILE_SINKS)