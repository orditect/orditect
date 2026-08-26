"""Data-rule toolkit for orditect-protocol (verification library).

This package is a verification toolkit — NOT part of the storage contract
surface (no domains, no CapabilitySet flags). It validates serialized data
products against the DR rules defined in docs/data-rules.md.

Tooling verbs like run_rules are toolkit verbs, not storage-contract verbs
(same rationale as run_conformance).
"""

from orditect.protocol.rules._types import Finding, Level, RuleReport
from orditect.protocol.rules.runner import run_rules

__all__ = ["Finding", "Level", "RuleReport", "run_rules"]