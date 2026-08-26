"""result_consumed dedup + invalidate_exempt_snapshot (DependencyGovernor)."""

from __future__ import annotations

import pytest

from orditect.flow.governance import DependencyGovernor

from fake_infra import FakeGovernanceStorage

pytestmark = pytest.mark.unit


class RecordingAuditWriter:
    def __init__(self) -> None:
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)


def _gov(storage, **kwargs) -> DependencyGovernor:
    kwargs.setdefault("success_words", frozenset({"succeeded"}))
    return DependencyGovernor(storage, **kwargs)


async def test_first_consumption_writes_audit():
    storage = FakeGovernanceStorage()
    audit = RecordingAuditWriter()
    gov = _gov(storage, audit_writer=audit)

    await gov.result_consumed("t1", "consumer-A")

    assert len(audit.events) == 1
    ev = audit.events[0]
    assert ev.event_type == "result_consumed"
    assert ev.event_id == "consume-t1-consumer-A"
    assert ev.task_id == "t1"
    assert ev.payload == {"consumer": "consumer-A"}


async def test_repeat_consumption_silent():
    storage = FakeGovernanceStorage()
    audit = RecordingAuditWriter()
    gov = _gov(storage, audit_writer=audit)

    await gov.result_consumed("t1", "consumer-A")
    await gov.result_consumed("t1", "consumer-A")
    await gov.result_consumed("t1", "consumer-A")

    assert len(audit.events) == 1  # dedup: no second event


async def test_distinct_consumers_each_audited():
    storage = FakeGovernanceStorage()
    audit = RecordingAuditWriter()
    gov = _gov(storage, audit_writer=audit)

    await gov.result_consumed("t1", "consumer-A")
    await gov.result_consumed("t1", "consumer-B")

    assert len(audit.events) == 2


async def test_no_audit_writer_no_error():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)  # audit_writer=None

    await gov.result_consumed("t1", "consumer-A")
    await gov.result_consumed("t1", "consumer-A")  # still dedups, never raises


async def test_invalidate_snapshot_resets_to_none():
    storage = FakeGovernanceStorage()
    gov = _gov(storage)
    await storage.initialize_task("c", "pending")
    await storage.update_task("c", {"exempt_resources_snapshot": ["llm"]})

    await gov.invalidate_exempt_snapshot("c")

    rec = await storage.get_task("c")
    assert rec["exempt_resources_snapshot"] is None