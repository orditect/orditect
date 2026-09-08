"""GovernedCallClient.call_streaming evidence-path pins.

Locks the interrupted/truncated stream accounting semantics:
- payload_fn runs on EVERY audited outcome (ok / error / cancelled),
  so bridge-side termination evidence always lands on the record;
- GeneratorExit without a flipped cancel token = external interruption:
  the partial result is captured and charged (tokens were consumed);
- GeneratorExit with a flipped token = true cancellation: partial
  pointer, cancelled flag, no charge (mirrors call()'s cancel path);
- handler error: partial result charged and marked charged_on_error.
"""
from __future__ import annotations

import pytest

from orditect.flow.governor.call import GovernedCallClient


class _FakeGovernor:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        self.acquired += 1
        return f"tok-{self.acquired}"

    async def release(self, resource: str, token: str) -> None:
        self.released += 1


class _FakeBudget:
    def __init__(self) -> None:
        self.charges: list[tuple[int, str]] = []

    async def check(self) -> None:
        return None

    async def charge(self, units: int, *, call_id: str) -> None:
        self.charges.append((units, call_id))


class _FakeAuditWriter:
    def __init__(self) -> None:
        self.events: list = []

    async def append(self, event) -> None:
        self.events.append(event)


class _Token:
    def __init__(self, cancelled: bool = False) -> None:
        self._cancelled = cancelled

    async def is_cancelled(self) -> bool:
        return self._cancelled


def _make_client(*, budget=None, audit=None) -> GovernedCallClient:
    return GovernedCallClient(
        _FakeGovernor(),
        "llm",
        budget=budget,
        cost_fn=lambda result: (result or {}).get("units", 0),
        audit_writer=audit,
        event_type="llm_call",
    )


async def _gen_ok():
    yield "a"
    yield "b"


async def _gen_fail():
    yield "a"
    raise RuntimeError("upstream closed")


class TestPayloadFnOnEveryOutcome:
    @pytest.mark.asyncio
    async def test_payload_fn_runs_on_error(self):
        audit = _FakeAuditWriter()
        client = _make_client(audit=audit)
        gen = client.call_streaming(
            handler=_gen_fail,
            payload_fn=lambda r: {"termination": "truncated"},
            call_id="c-err",
        )
        with pytest.raises(RuntimeError):
            async for _ in gen:
                pass
        assert audit.events[-1].payload["termination"] == "truncated"
        assert "error" in audit.events[-1].payload

    @pytest.mark.asyncio
    async def test_payload_fn_runs_on_ok(self):
        audit = _FakeAuditWriter()
        client = _make_client(audit=audit)
        async for _ in client.call_streaming(
                handler=_gen_ok,
                payload_fn=lambda r: {"termination": "completed"},
                call_id="c-ok"):
            pass
        assert audit.events[-1].payload["termination"] == "completed"


class TestGeneratorExitClassification:
    @pytest.mark.asyncio
    async def test_break_without_token_charges_partial(self):
        """External interruption (no token): partial result is charged."""
        budget = _FakeBudget()
        audit = _FakeAuditWriter()
        client = _make_client(budget=budget, audit=audit)
        stream = client.call_streaming(
            handler=_gen_ok,
            result_fn=lambda: {"units": 7},
            call_id="c-int",
        )
        async for _ in stream:
            break  # consumer abandons the stream, no cancel token
        await stream.aclose()
        assert budget.charges == [(7, "c-int")]
        payload = audit.events[-1].payload
        assert not payload.get("cancelled")
        assert payload["cost_units"] == 7

    @pytest.mark.asyncio
    async def test_break_with_cancelled_token_marks_cancelled_no_charge(self):
        """True cancellation (token flips mid-stream): no charge."""
        budget = _FakeBudget()
        audit = _FakeAuditWriter()
        client = _make_client(budget=budget, audit=audit)
        token = _Token(cancelled=False)
        stream = client.call_streaming(
            handler=_gen_ok,
            result_fn=lambda: {"units": 7},
            cancel_token=token,
            call_id="c-cancel",
        )
        async for _ in stream:
            token._cancelled = True  # cancel arrives mid-stream
            break
        await stream.aclose()
        assert budget.charges == []
        assert audit.events[-1].payload["cancelled"] is True


class TestErrorPathCharge:
    @pytest.mark.asyncio
    async def test_handler_error_charges_partial_and_marks(self):
        budget = _FakeBudget()
        audit = _FakeAuditWriter()
        client = _make_client(budget=budget, audit=audit)
        gen = client.call_streaming(
            handler=_gen_fail,
            result_fn=lambda: {"units": 5},
            call_id="c-fail",
        )
        with pytest.raises(RuntimeError):
            async for _ in gen:
                pass
        assert budget.charges == [(5, "c-fail")]
        payload = audit.events[-1].payload
        assert payload["charged_on_error"] is True
        assert payload["cost_units"] == 5

    @pytest.mark.asyncio
    async def test_error_without_result_charges_nothing(self):
        budget = _FakeBudget()
        audit = _FakeAuditWriter()
        client = _make_client(budget=budget, audit=audit)
        gen = client.call_streaming(
            handler=_gen_fail,
            result_fn=lambda: None,
            call_id="c-noresult",
        )
        with pytest.raises(RuntimeError):
            async for _ in gen:
                pass
        assert budget.charges == []
        assert "charged_on_error" not in audit.events[-1].payload

    @pytest.mark.asyncio
    async def test_semaphore_released_on_every_path(self):
        gov = _FakeGovernor()
        client = GovernedCallClient(gov, "llm")
        gen = client.call_streaming(handler=_gen_fail, call_id="c-rel")
        with pytest.raises(RuntimeError):
            async for _ in gen:
                pass
        assert gov.acquired == 1
        assert gov.released == 1