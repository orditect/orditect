"""Shared types for the data-rule toolkit."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Level = Literal["violation", "warning"]


@dataclass(frozen=True)
class Finding:
    """One rule finding (data-rules.md §3.4: id, level, location, message, term)."""

    rule: str           # e.g. "DR-SNP-001"
    level: Level
    location: str       # e.g. "snapshots[3].data"
    message: str
    term: str           # e.g. "T3"
    degraded: bool = False  # input lacked op; rule ran with reduced strength


@dataclass
class RuleReport:
    """Aggregated rule findings."""

    findings: list[Finding] = field(default_factory=list)

    @property
    def violation_count(self) -> int:
        return sum(1 for f in self.findings if f.level == "violation")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.level == "warning")

    @property
    def ok(self) -> bool:
        """True when no violation was found (warnings do not fail)."""
        return self.violation_count == 0

    def summary(self) -> str:
        lines = [
            f"data-rules: {self.violation_count} violations, "
            f"{self.warning_count} warnings"
        ]
        for f in self.findings:
            tag = "degraded " if f.degraded else ""
            lines.append(
                f"  {f.level.upper():9s} {f.rule} ({f.term}) "
                f"{tag}at {f.location}: {f.message}"
            )
        return "\n".join(lines)