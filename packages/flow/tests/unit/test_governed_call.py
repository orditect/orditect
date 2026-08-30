"""Pinning tests for GovernedCallClient (B1).

Covers: audit-once-per-call with event_id == call_id, error / cancel-skip
paths, content pointer-ization, opaque-label passthrough, streaming semantics
(charge at stream end, audit at stream close, sem held for the stream's whole
lifetime, cancel cleanup) and the usage-missing pricing path.
"""

from __future__ import annotations
import asyncio
import pytest

from orditect.flow import BudgetExhaustedError, GovernedCallClient
from orditect.protocol import TaskPointer

pytestmark = pytest.mark.unit


# ---------- test doubles ----------


class FakeGovernor:
    def __init__(self):
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout=None) -> str:
        self.acquired.append(resource)
        return f"tok-{len(self.acquired)}"

    async def try_acquire(self, resource: str):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return 0


class RecordingAudit:
    def __init__(self):
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)


class FailingAudit:
    async def append(self, event) -> None:
        raise RuntimeError("sink down")


class FakeContentWriter:
    def __init__(self):
        self.blobs: dict[str, bytes] = {}
        self._seq = 0

    async def put(self, content: bytes, *, content_type=None, metadata=None):
        self._seq += 1
        key = f"mem://c/{self._seq}"
        self.blobs[key] = content
        return TaskPointer(backend="mem", key=key)


class FakeBudget:
    def __init__(self, balance: int = 10):
        self.balance = balance
        self.charges: list[tuple[str, int]] = []

    async def check(self) -> None:
        if self.balance <= 0:
            raise BudgetExhaustedError("budget exhausted")

    async def charge(self, units: int, *, call_id: str) -> int:
        self.charges.append((call_id, units))
        self.balance -= units
        return self.balance


class CancelToken:
    def __init__(self, cancelled: bool):
        self._cancelled = cancelled

    async def is_cancelled(self) -> bool:
        return self._cancelled


async def _ok():
    return "ok"


# ---------- non-streaming ----------


class TestCallObservation:
    async def test_success_writes_one_audit_with_call_id(self):
        audit = RecordingAudit()
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok,
            audit_writer=audit, event_type="unit_call", task_id="t-1",
        )
        result = await client.call(call_id="c-1")

        assert result == "ok"
        assert len(audit.events) == 1
        ev = audit.events[0]
        assert ev.event_id == "c-1"
        assert ev.task_id == "t-1"
        assert ev.event_type == "unit_call"
        assert "elapsed_ms" in ev.payload
        assert "error" not in ev.payload
        assert "cancelled" not in ev.payload

    async def test_default_call_id_generated(self):
        audit = RecordingAudit()
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok, audit_writer=audit
        )
        await client.call()
        assert audit.events[0].event_id.startswith("call-")

    async def test_error_audited_and_propagates(self):
        audit = RecordingAudit()

        async def boom():
            raise ValueError("nope")

        client = GovernedCallClient(
            FakeGovernor(), "res", handler=boom, audit_writer=audit
        )
        with pytest.raises(ValueError, match="nope"):
            await client.call(call_id="c-2")

        assert audit.events[0].event_id == "c-2"
        assert audit.events[0].payload["error"] == "nope"

    async def test_cancelled_before_acquire_leaves_no_record(self):
        audit = RecordingAudit()
        gov = FakeGovernor()
        client = GovernedCallClient(
            gov, "res", handler=_ok, audit_writer=audit
        )
        result = await client.call(cancel_token=CancelToken(True))

        assert result is None
        assert gov.acquired == []
        assert audit.events == []

    async def test_content_pointerized_and_recorded(self):
        content = FakeContentWriter()
        audit = RecordingAudit()
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok,
            audit_writer=audit, content_writer=content,
        )
        await client.call(content_fn=lambda r: b"big-bytes")

        ev = audit.events[0]
        assert ev.payload["pointer"]["backend"] == "mem"
        assert list(content.blobs.values()) == [b"big-bytes"]

    async def test_opaque_labels_in_payload(self):
        audit = RecordingAudit()
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok, audit_writer=audit,
            parent_task_id="p-1", execution_id="exec-1",
        )
        await client.call()

        assert audit.events[0].payload["parent_task_id"] == "p-1"
        assert audit.events[0].payload["execution_id"] == "exec-1"

    async def test_audit_failure_never_blocks(self):
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok, audit_writer=FailingAudit()
        )
        assert await client.call() == "ok"

    async def test_budget_blocked_no_audit(self):
        audit = RecordingAudit()
        client = GovernedCallClient(
            FakeGovernor(), "res", handler=_ok,
            budget=FakeBudget(balance=0), audit_writer=audit,
        )
        with pytest.raises(BudgetExhaustedError):
            await client.call()
        assert audit.events == []


# ---------- streaming ----------


class TestCallStreaming:
    async def test_stream_success_full_lifecycle(self):
        audit = RecordingAudit()
        budget = FakeBudget()
        gov = FakeGovernor()

        async def gen():
            for i in range(3):
                yield i

        client = GovernedCallClient(
            gov, "res", audit_writer=audit, budget=budget,
            cost_fn=lambda r: 5,
        )
        chunks = [
            c
            async for c in client.call_streaming(
                handler=lambda: gen(), result_fn=lambda: {"u": 5}, call_id="s-1"
            )
        ]

        assert chunks == [0, 1, 2]
        assert gov.acquired == ["res"]
        assert gov.released == ["res"]
        assert budget.charges == [("s-1", 5)]
        assert len(audit.events) == 1
        ev = audit.events[0]
        assert ev.event_id == "s-1"
        assert "error" not in ev.payload
        assert "cancelled" not in ev.payload

    async def test_stream_result_none_passed_to_cost_fn(self):
        seen: list = []
        budget = FakeBudget()

        async def gen():
            yield 1

        client = GovernedCallClient(
            FakeGovernor(), "res", budget=budget,
            cost_fn=lambda r: seen.append(r) or 2,
        )
        async for _ in client.call_streaming(
            handler=lambda: gen(), result_fn=lambda: None
        ):
            pass

        # A5: usage missing -> cost_fn receives None; the business prices it.
        assert seen == [None]
        assert budget.charges[0][1] == 2

    async def test_stream_break_marks_cancelled_and_pointerizes_partial(self):
        import asyncio
        audit = RecordingAudit()
        content = FakeContentWriter()

        async def gen():
            for i in range(100):
                yield i

        client = GovernedCallClient(
            FakeGovernor(), "res", audit_writer=audit, content_writer=content
        )
        count = 0
        async for _ in client.call_streaming(
            handler=lambda: gen(), partial_fn=lambda: b"partial-data"
        ):
            count += 1
            if count == 2:
                break

        # break triggers GeneratorExit, whose finally block (audit write)
        # runs on the NEXT event-loop tick. Yield control so the generator's
        # cleanup completes before asserting.
        for _ in range(50):
            if audit.events:
                break
            await asyncio.sleep(0.01)

        ev = audit.events[0]
        assert ev.payload["cancelled"] is True
        assert ev.payload["pointer"]["backend"] == "mem"
        assert list(content.blobs.values()) == [b"partial-data"]

    async def test_stream_error_audited_and_released(self):
        audit = RecordingAudit()
        gov = FakeGovernor()

        async def gen():
            yield 1
            raise RuntimeError("mid-fail")

        client = GovernedCallClient(gov, "res", audit_writer=audit)
        with pytest.raises(RuntimeError, match="mid-fail"):
            async for _ in client.call_streaming(handler=lambda: gen()):
                pass

        assert audit.events[0].payload["error"] == "mid-fail"
        assert gov.released == ["res"]

    async def test_stream_budget_blocked_no_acquire_no_audit(self):
        audit = RecordingAudit()
        gov = FakeGovernor()

        async def gen():
            yield 1

        client = GovernedCallClient(
            gov, "res", budget=FakeBudget(balance=0), audit_writer=audit
        )
        with pytest.raises(BudgetExhaustedError):
            async for _ in client.call_streaming(handler=lambda: gen()):
                pass

        assert gov.acquired == []
        assert audit.events == []

class TestStreamingReleaseStrongRef:
    async def test_stream_release_completes_and_drains(self):
        """v0.1.6 pinning: the streaming path's shielded release completes
        and the strong-ref set drains (no orphaned shield task)."""
        import time

        class SlowGovernor(FakeGovernor):
            async def release(self, resource: str, token: str) -> None:
                await asyncio.sleep(0.1)
                await super().release(resource, token)

        async def gen():
            yield 1

        governor = SlowGovernor()
        client = GovernedCallClient(governor, "res")

        async for _ in client.call_streaming(handler=lambda: gen()):
            pass

        # release is shielded; wait for it to land and the set to drain
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if governor.released == ["res"] and not client._release_tasks:
                break
            await asyncio.sleep(0.02)

        assert governor.released == ["res"]
        assert client._release_tasks == set()

class TestStreamingInnerGeneratorClosed:
    """v0.1.7 pinning (issue #4): the handler's generator is deterministically
    closed on consumer break — inner finally blocks (resource cleanup) run
    immediately, not at GC time.

    Red before: call_streaming iterated fn() without holding a reference and
    never aclosed it; breaking out left the inner generator's cleanup to
    asyncio asyncgen finalization (GC timing).
    """

    async def test_break_acloses_inner_generator_deterministically(self):
        closed: list[bool] = []

        async def gen():
            try:
                for i in range(100):
                    yield i
            finally:
                closed.append(True)

        client = GovernedCallClient(FakeGovernor(), "res")
        stream = client.call_streaming(handler=lambda: gen())
        async for _ in stream:
            break
        await stream.aclose()

        assert closed == [True]

    async def test_normal_completion_acloses_inner_generator_too(self):
        """The aclose is unconditional (a fully-consumed generator closes
        cleanly as well)."""
        closed: list[bool] = []

        async def gen():
            try:
                yield 1
            finally:
                closed.append(True)

        client = GovernedCallClient(FakeGovernor(), "res")
        async for _ in client.call_streaming(handler=lambda: gen()):
            pass

        assert closed == [True]