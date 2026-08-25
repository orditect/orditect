"""Conformance test kit for orditect-protocol implementations.

Usage (in an adapter's own test suite):

    from orditect.protocol.conformance import run_conformance

    def test_conformance():
        results = run_conformance(my_adapter_instance)
        assert results.failed == 0, results.summary()

The kit reads the adapter's CapabilitySet and automatically skips undeclared
half-domains (term T8 — skip, not fail). Every case's docstring starts with
its case id (CF-XXX-NNN) and the term(s) it verifies, closing the
traceability loop with docs/terms.md Appendix A/B.
"""

from orditect.protocol.conformance.runner import (
    ConformanceReport,
    ConformanceResult,
    run_conformance,
)

__all__ = [
    "run_conformance",
    "ConformanceReport",
    "ConformanceResult",
]