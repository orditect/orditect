"""Run the orditect-protocol conformance suite against MemoryStore parts.

The suite is invoked per-domain-part (each part exposes its own
CapabilitySet), which is the recommended pattern for adapters structured as
per-domain compositions.
"""

import pytest

from orditect.adapter.memory import MemoryStore
from orditect.protocol.conformance import run_conformance


class TestMemoryConformance:
    def test_content_part(self):
        report = run_conformance(MemoryStore().content)
        assert report.failed == 0, report.summary()

    def test_audit_part(self):
        report = run_conformance(MemoryStore().audit)
        assert report.failed == 0, report.summary()

    def test_result_part(self):
        report = run_conformance(MemoryStore().result)
        assert report.failed == 0, report.summary()

    def test_snapshot_part(self):
        report = run_conformance(MemoryStore().snapshot)
        assert report.failed == 0, report.summary()

    def test_dependency_part(self):
        report = run_conformance(MemoryStore().dependency)
        assert report.failed == 0, report.summary()