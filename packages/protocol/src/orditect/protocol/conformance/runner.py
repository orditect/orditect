"""Conformance runner: discovers and executes capability-gated cases.

Design:
- Cases are grouped by half-domain; a case runs only when the adapter
  declares the corresponding capability (term T8).
- Each case is an async callable receiving the adapter under test.
- Results are collected (not raised) so an adapter gets a full report.
"""

from __future__ import annotations

import asyncio
import traceback
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.capabilities import CapabilitySet
from orditect.protocol.conformance.profiles import PROFILES

CaseFn = Callable[[Any], Awaitable[None]]


@dataclass
class ConformanceResult:
    """Outcome of a single conformance case."""

    case_id: str
    half_domain: str
    status: str  # "passed" | "failed" | "skipped"
    detail: str = ""

@dataclass
class ConformanceReport:
    """Aggregated conformance results."""

    results: list[ConformanceResult] = field(default_factory=list)
    profile: str = "full"
    eligibility_error: str | None = None  # set => the whole tier is ineligible

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.status == "passed")

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.status == "failed")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "skipped")

    def summary(self) -> str:
        if self.eligibility_error is not None:
            return (
                f"conformance[{self.profile}]: INELIGIBLE — "
                f"{self.eligibility_error}"
            )
        lines = [
            f"conformance[{self.profile}]: {self.passed} passed, "
            f"{self.failed} failed, {self.skipped} skipped"
        ]
        for r in self.results:
            if r.status == "failed":
                lines.append(f"  FAILED {r.case_id}: {r.detail}")
        return "\n".join(lines)

@dataclass
class _Case:
    case_id: str
    half_domain: str
    fn: CaseFn

def run_conformance(adapter: Any, *, profile: str = "full") -> ConformanceReport:
    """Run the conformance suite against an adapter under a profile.

    Profiles (docs/conformance.md): "full" (paired sink/query required),
    "producer" (sinks as declared), "consumer" (queries + seeded CF-VIEW
    cases). A profile is a minimum bar, not a ceiling: any half-domain the
    adapter declares is verified regardless of profile (T8).

    Args:
        adapter: an object exposing `capabilities: CapabilitySet` plus the
            methods of whichever half-domains it declares. For the consumer
            profile it may additionally implement `seed(fixtures)` to enable
            the CF-VIEW seeded cases.
        profile: "full" | "producer" | "consumer".

    Returns:
        ConformanceReport; for full-tier pairing violations the report
        carries eligibility_error and no case results (an eligibility
        problem is not a case failure).
    """
    if profile not in PROFILES:
        raise ValueError(f"unknown conformance profile: {profile!r}")
    return asyncio.run(_run_all(adapter, profile))


def _check_pairing(caps: CapabilitySet) -> str | None:
    """Full-tier eligibility: sink/query must be declared in pairs (T8)."""
    for domain in ("content", "audit", "result", "snapshot", "dependency"):
        sink, query = f"{domain}_sink", f"{domain}_query"
        if caps.supports(sink) != caps.supports(query):
            declared = sink if caps.supports(sink) else query
            return (
                f"full profile requires paired sink/query declarations; "
                f"declared {declared} without its pair (T8)"
            )
    return None


async def _run_all(adapter: Any, profile: str) -> ConformanceReport:
    """Single-loop execution of the suite under one profile."""
    from orditect.protocol.conformance.profiles import (
        PROFILE_REQUIRES_PAIRING,
        PROFILE_SINKS,
        VIEW_DOMAIN,
    )

    caps: CapabilitySet = getattr(adapter, "capabilities", CapabilitySet())
    report = ConformanceReport(profile=profile)

    if PROFILE_REQUIRES_PAIRING[profile]:
        error = _check_pairing(caps)
        if error:
            report.eligibility_error = error
            return report

    # Consumer tier: seed fixtures when the adapter implements the
    # (extra-contract) seed hook; otherwise CF-VIEW cases degrade to skip.
    seeded = False
    if profile == "consumer":
        seed = getattr(adapter, "seed", None)
        if callable(seed):
            from orditect.protocol.conformance.fixtures import consumer_fixtures
            await seed(consumer_fixtures())
            seeded = True

    for case in _all_cases():
        if case.half_domain == VIEW_DOMAIN:
            if profile != "consumer":
                continue
            if not seeded:
                report.results.append(ConformanceResult(
                    case.case_id, VIEW_DOMAIN, "skipped",
                    "seed not implemented; consumer verification limited"))
                continue
        else:
            if case.half_domain not in PROFILE_SINKS[profile]:
                continue
            if not caps.supports(case.half_domain):
                report.results.append(ConformanceResult(
                    case.case_id, case.half_domain, "skipped",
                    "capability not declared (T8)"))
                continue
        try:
            await case.fn(adapter)
        except Exception as exc:  # noqa: BLE001 — collect, never raise
            report.results.append(
                ConformanceResult(case.case_id, case.half_domain, "failed",
                                  f"{type(exc).__name__}: {exc}\n"
                                  f"{traceback.format_exc(limit=3)}")
            )
        else:
            report.results.append(
                ConformanceResult(case.case_id, case.half_domain, "passed")
            )
    return report

def _all_cases() -> list[_Case]:
    """Collect all conformance cases (imported lazily to avoid cycles)."""
    from orditect.protocol.conformance import (
        cases_audit,
        cases_content,
        cases_dependency,
        cases_result,
        cases_snapshot,
        cases_view,
    )

    cases: list[_Case] = []
    for module in (cases_content, cases_audit, cases_result, cases_snapshot,
                   cases_dependency, cases_view):
        for case_id, half_domain, fn in module.CASES:
            cases.append(_Case(case_id, half_domain, fn))
    return cases