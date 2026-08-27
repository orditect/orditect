
"""Run the orditect-protocol conformance suite against LocalFileStore parts
(full profile: five domains, sink/query declared in pairs)."""

import pytest

from orditect.adapter.local import LocalFileStore
from orditect.protocol.conformance import run_conformance


@pytest.fixture
def store(tmp_path):
    return LocalFileStore(tmp_path / "store")


class TestLocalFileConformance:
    def test_content_part(self, store):
        report = run_conformance(store.content, profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_audit_part(self, store):
        report = run_conformance(store.audit, profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_result_part(self, store):
        report = run_conformance(store.result, profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_snapshot_part(self, store):
        report = run_conformance(store.snapshot, profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_dependency_part(self, store):
        report = run_conformance(store.dependency, profile="full")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()