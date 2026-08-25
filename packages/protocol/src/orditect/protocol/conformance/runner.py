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
        lines = [
            f"conformance: {self.passed} passed, {self.failed} failed, "
            f"{self.skipped} skipped"
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


def run_conformance(adapter: Any) -> ConformanceReport:
    """Run the full conformance suite against an adapter.

    Args:
        adapter: an object exposing `capabilities: CapabilitySet` plus the
            methods of whichever half-domains it declares.

    Returns:
        ConformanceReport with one ConformanceResult per case.
    """
    caps: CapabilitySet = getattr(adapter, "capabilities", CapabilitySet())
    report = ConformanceReport()

    for case in _all_cases():
        if not caps.supports(case.half_domain):
            report.results.append(
                ConformanceResult(case.case_id, case.half_domain, "skipped",
                                  "capability not declared (T8)")
            )
            continue
        try:
            asyncio.run(case.fn(adapter))
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
        cases_result,
        cases_snapshot,
    )

    cases: list[_Case] = []
    for module in (cases_content, cases_audit, cases_result, cases_snapshot):
        for case_id, half_domain, fn in module.CASES:
            cases.append(_Case(case_id, half_domain, fn))
    return cases