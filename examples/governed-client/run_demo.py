"""Orditect governed-client demo: wrap ANY existing callable in governance.

No workflow, no orchestrator — this example shows how to embed Orditect
into code you ALREADY have. Four stages:

  1 GovernedClient (non-streaming): semaphore + budget + call_id idempotency
  2 GovernedCallClient (non-streaming): + audit events + content pointer-ization
  3 GovernedCallClient (streaming): sem held for the stream's lifetime,
    audit at stream close, cost charged at stream end (incl. the A5
    usage-missing pricing path)
  4 validate: budget balance + audit log tell the whole story (zero infra)

Run from the repository root:
    python examples/governed-client/run_demo.py
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bootstrap  # noqa: F401,E402  (must precede every orditect import)

import httpx  # noqa: E402

from orditect.adapter.local import LocalFileStore  # noqa: E402
from orditect.flow import (  # noqa: E402
    BudgetExhaustedError,
    BudgetLedger,
    GovernedCallClient,
    GovernedClient,
)

from infra import InMemoryGovernor, InMemoryQuota  # noqa: E402

DEMO_DIR = Path(__file__).resolve().parent / "demo_data"
TRACE_DIR = DEMO_DIR / "trace"


def make_http_llm():
    """A plain pre-existing async callable (the kind you already have)."""

    async def mock_endpoint(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "chatcmpl-demo",
            "model": "gpt-4o-mock",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "mock-llm-answer"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 12, "completion_tokens": 18, "total_tokens": 30},
        })

    http = httpx.AsyncClient(transport=httpx.MockTransport(mock_endpoint))

    async def call_llm(prompt: str) -> dict:
        resp = await http.post(
            "http://mock/chat/completions",
            json={"model": "gpt-4o-mock",
                  "messages": [{"role": "user", "content": prompt}]},
        )
        return resp.json()

    return call_llm


def _tokens_cost(result) -> int:
    """Price by real token usage; None (usage-missing stream) prices at 0 (A5)."""
    if not isinstance(result, dict):
        return 0
    usage = result.get("usage") or {}
    return int(usage.get("total_tokens", 0))


def _request_bytes(prompt: str):
    """content_fn factory: pointer-ize the outgoing request body.

    Returns a ContentFn (result -> bytes | None); pointer-ization happens
    only when content_writer is set AND a content_fn/partial_fn is given.
    """

    def fn(result) -> bytes | None:
        return json.dumps(
            [{"role": "user", "content": prompt}], ensure_ascii=False
        ).encode("utf-8")

    return fn


def _read_audit_events() -> list[dict]:
    audit_file = TRACE_DIR / "audit.ndjson"
    if not audit_file.is_file():
        return []
    return [json.loads(x)["data"] for x in audit_file.read_text().splitlines()]


async def main() -> None:
    shutil.rmtree(DEMO_DIR, ignore_errors=True)

    # ---- setup: in-memory hot path + a real local trace bundle --------
    store = LocalFileStore(TRACE_DIR)
    governor = InMemoryGovernor(capacity=2)
    budget = BudgetLedger(InMemoryQuota(), root_task_id="governed-demo", max_units=100)
    await budget.open()

    call_llm = make_http_llm()

    # ---- 1. GovernedClient: semaphore + budget + call_id --------------
    print("==> 1. GovernedClient (non-streaming)")
    client = GovernedClient(
        governor, resource="llm", handler=call_llm,
        budget=budget,
        cost_fn=_tokens_cost,
    )
    await client.call("hello", call_id="call-1")
    print(f"balance after call-1: {await budget.balance()}")
    await client.call("hello", call_id="call-1")  # same logical call retried
    print(f"balance after retry with same call_id (deduped): {await budget.balance()}")
    assert await budget.balance() == 70  # 100 - 30, charged exactly once

    # ---- 2. GovernedCallClient: + audit + content pointer-ization -----
    print("\n==> 2. GovernedCallClient (non-streaming: audit + pointer)")
    observed = GovernedCallClient(
        governor, resource="llm", handler=call_llm,
        budget=budget,
        cost_fn=_tokens_cost,
        audit_writer=store.audit,
        content_writer=store.content,
        event_type="llm_call",
        task_id="governed-demo",
        content_type="application/json",
    )
    await observed.call(
        "audit me",
        call_id="call-2",
        content_fn=_request_bytes("audit me"),
    )
    ev = _read_audit_events()[-1]
    print(f"audit event: id={ev['event_id']} cost_units={ev['payload']['cost_units']} "
          f"elapsed_ms={ev['payload']['elapsed_ms']}")
    assert ev["event_id"] == "call-2"
    assert ev["payload"]["cost_units"] == 30
    assert "pointer" in ev["payload"]  # request messages pointer-ized

    # ---- 3. GovernedCallClient: streaming lifecycle -------------------
    print("\n==> 3. GovernedCallClient (streaming)")

    async def token_stream():
        for tok in ("Hello", ", ", "world"):
            yield tok
        # no usage reported by this source (A5 path)

    chunks: list[str] = []
    stream = observed.call_streaming(
        handler=lambda: token_stream(),
        call_id="stream-1",
        result_fn=lambda: None,  # source reports no usage -> cost_fn(None)
    )
    async for chunk in stream:
        chunks.append(chunk)
        if len(chunks) == 1:
            in_flight = await governor.get_usage("llm")
            print(f"llm usage mid-stream (sem held for the whole lifetime): {in_flight}")
            assert in_flight == 1
    await asyncio.sleep(0)  # let the finally chain (charge + audit) settle
    usage_after = await governor.get_usage("llm")
    print(f"llm usage after stream closed (released): {usage_after}")
    assert usage_after == 0

    stream_events = [e for e in _read_audit_events() if e["event_id"] == "stream-1"]
    assert len(stream_events) == 1
    print(f"stream audit event written at close: id={stream_events[0]['event_id']}")
    # A5: result_fn() was None -> cost_fn(None) -> 0 units -> charge(0) no-op.
    assert await budget.balance() == 40  # 70 - 30 (call-2), stream charged 0

    # ---- budget exhaustion: blocked BEFORE acquire ---------------------
    print("\n==> budget exhaustion blocks before acquire")
    # check() blocks only when balance <= 0: the post-charge model lets the
    # last call overspend honestly, and every SUBSEQUENT check then blocks.
    # So we pre-drain the ledger, then the next call is refused before acquire.
    tiny = BudgetLedger(InMemoryQuota(), root_task_id="tiny", max_units=100)
    await tiny.open()
    await tiny.charge(150, call_id="seed-overspend")  # balance now -50
    assert await tiny.balance() == -50
    blocked_client = GovernedClient(
        governor, resource="llm", handler=call_llm,
        budget=tiny, cost_fn=_tokens_cost,
    )
    try:
        await blocked_client.call("too expensive", call_id="blocked-1")
    except BudgetExhaustedError:
        print("BudgetExhaustedError raised before acquire (expected)")
    else:
        raise AssertionError("expected BudgetExhaustedError")

    # ---- 4. summary ----------------------------------------------------
    print(f"\nfinal budget balance: {await budget.balance()} / 100")
    print(f"audit events total: {len(_read_audit_events())}")
    print("\nDEMO OK - trace bundle at:", TRACE_DIR)


if __name__ == "__main__":
    asyncio.run(main())