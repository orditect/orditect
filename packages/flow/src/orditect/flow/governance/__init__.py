"""Dependency governance plane (v0.1.1): passive multi-parent dependency APIs."""

from orditect.flow.governance.dependency import DependencyGovernor
from orditect.flow.governance.tools import (
    rebuild_dep_counters,
    scan_dependency_cycles,
)

__all__ = ["DependencyGovernor", "scan_dependency_cycles", "rebuild_dep_counters"]