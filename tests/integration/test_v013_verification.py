"""v0.1.3 verification test suite.

Runs all certification checks for the three-category protocolization:
- adapter-local: full conformance profile
- bridge-openai: producer conformance profile
- adapter-ui: consumer + action profiles
- End-to-end governance loop
- Swap tests (adapters/bridges interchangeable)
- run_rules validation on trace bundles
"""

import asyncio
import json

import httpx
import pytest

from orditect.adapter.local import LocalFileStore
from orditect.adapter.memory import MemoryStore
from orditect.adapter.ui import (
    ActionSinkAdapter,
    MemoryActionQueue,
    TraceBundleReader,
)
from orditect.bridge.openai import GovernedLLMClient
from orditect.flow import BudgetLedger, TaskOrchestrator
from orditect.flow.actions import ActionDispatcher
from orditect.protocol import AuditEvent, TaskSnapshot
from orditect.protocol.conformance import run_conformance
from orditect.protocol.rules import run_rules

pytestmark = pytest.mark.integration

# ---------- Test Infrastructure ----------


class FakeGovernor:
    """In-memory governor with configurable capacity."""

    def __init__(self, capacity: int = 2):
        self.capacity = capacity
        self.in_use = 0
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout=None) -> str:
        if self.in_use >= self.capacity:
            from orditect.flow.exceptions import AcquireTimeoutError
            raise AcquireTimeoutError(f"resource full: {resource}")
        self.in_use += 1
        self.acquired.append(resource)
        return f"tok-{len(self.acquired)}"

    async def try_acquire(self, resource):
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.in_use = max(0, self.in_use - 1)
        self.released.append(resource)

    async def get_usage(self, resource: str) -> int:
        return self.in_use


class FakeQuota:
    """In-memory quota DB."""

    def __init__(self):
        self._pending = 0

    async def reserve_units(self, **kw):
        if self._pending + kw["units"] > kw["max_units"]:
            return {"ok": False, "reason": "limit_exceeded"}
        self._pending += kw["units"]
        return {"ok": True}

    async def get_pending_units(self, **kw):
        return self._pending


class FakeStorage:
    """In-memory task storage for orchestrator."""

    def __init__(self):
        self._tasks = {}

    async def initialize_task(self, task_id, initial_status, **kw):
        self._tasks[task_id] = {
            "task_id": task_id,
            "status": initial_status,
            "cancel_requested": False,
            "execution_id": f"exec-{task_id}",
        }
        return True

    async def update_task(self, task_id, updates, **kwargs):
        if task_id in self._tasks:
            self._tasks[task_id].update(updates)

    async def get_task(self, task_id):
        return dict(self._tasks.get(task_id, {}))

    async def request_cancel(self, task_id):
        if task_id not in self._tasks:
            return False
        self._tasks[task_id]["cancel_requested"] = True
        return True

    async def list_children(self, parent_task_id):
        return [
            tid for tid, t in self._tasks.items()
            if t.get("parent_task_id") == parent_task_id
        ]

    async def reopen_task(self, task_id, **kw):
        new_eid = f"exec-re-{task_id}"
        self._tasks[task_id]["execution_id"] = new_eid
        self._tasks[task_id]["status"] = "pending"
        self._tasks[task_id].pop("result", None)
        return new_eid


class FakeReader:
    """Fake snapshot reader for RecoveryService."""

    def __init__(self, statuses, tree_ids):
        self._statuses = statuses
        self._tree_ids = tree_ids

    async def get(self, task_id, step="execute"):
        st = self._statuses.get(task_id)
        if st is None:
            return None

        class S:
            def __init__(self, tid, status):
                self.task_id = tid
                self.status = status

        return S(task_id, st)

    async def get_tree(self, root_task_id, **kw):
        return [
            type("S", (), {"task_id": t, "status": self._statuses.get(t, "")})()
            for t in self._tree_ids
        ]


class FakeExecutor:
    """Fake executor for RecoveryService."""

    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, task_id, task, **kw):
        self.executed.append(task_id)
        return {"re": task_id}


class _NoopTask:
    async def execute(self, task_id, **kwargs):
        return None


async def _factory(task_id):
    return _NoopTask()


def _chat_response(tokens: int, model: str = "gpt-4o"):
    """Mock OpenAI chat completion response."""
    return {
        "id": "chatcmpl-1",
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "response"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": tokens // 2,
            "completion_tokens": tokens // 2,
            "total_tokens": tokens,
        },
    }


async def _wait_for(predicate, timeout=3.0, interval=0.02):
    """Wait for a condition to become true."""
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


# ---------- Certification Tests ----------


class TestAdapterLocalCertification:
    """adapter-local passes full conformance profile (all five domains).

    NOTE: run_conformance internally calls asyncio.run(), so these tests
    must be sync functions (pytest-asyncio's async tests already run
    inside an event loop; nesting asyncio.run would raise RuntimeError).
    """

    def test_content_part_full(self, tmp_path):
        report = run_conformance(
            LocalFileStore(tmp_path / "s").content, profile="full"
        )
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_audit_part_full(self, tmp_path):
        report = run_conformance(
            LocalFileStore(tmp_path / "s").audit, profile="full"
        )
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_result_part_full(self, tmp_path):
        report = run_conformance(
            LocalFileStore(tmp_path / "s").result, profile="full"
        )
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_snapshot_part_full(self, tmp_path):
        report = run_conformance(
            LocalFileStore(tmp_path / "s").snapshot, profile="full"
        )
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    def test_dependency_part_full(self, tmp_path):
        report = run_conformance(
            LocalFileStore(tmp_path / "s").dependency, profile="full"
        )
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

    async def test_trace_bundle_producer(self, tmp_path):
        """adapter-local produces valid trace bundles."""
        store = LocalFileStore(tmp_path / "trace")
        await store.snapshot.save(
            TaskSnapshot(
                task_id="t1", step="s1", execution_id="e1", status="done"
            )
        )
        assert (tmp_path / "trace" / "snapshots.ndjson").is_file()


class TestBridgeOpenAICertification:
    """bridge-openai passes producer conformance profile."""

    async def test_producer_profile(self, tmp_path):
        """bridge writes audit events via protocol sink."""
        store = LocalFileStore(tmp_path / "trace")

        async def mock_endpoint(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_chat_response(tokens=10))

        http = httpx.AsyncClient(transport=httpx.MockTransport(mock_endpoint))
        llm = GovernedLLMClient(
            "http://mock",
            governor=FakeGovernor(),
            resource="llm",
            audit_writer=store.audit,
            model="gpt-4o",
            http_client=http,
        )

        await llm.chat(messages=[{"role": "user", "content": "test"}])

        # Verify audit event was written via protocol sink
        events = []
        for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines():
            events.append(json.loads(line))
        assert len(events) == 1
        assert events[0]["data"]["event_type"] == "llm_call"


class TestAdapterUICertification:
    """adapter-ui passes consumer + action profiles."""

    async def test_consumer_profile(self, tmp_path):
        """TraceBundleReader reads trace bundles without orditect imports."""
        # Produce a trace bundle
        store = LocalFileStore(tmp_path / "trace")
        await store.snapshot.save(
            TaskSnapshot(
                task_id="t1", step="s1", execution_id="e1", status="done"
            )
        )

        # Read it with UI adapter
        reader = TraceBundleReader(tmp_path / "trace")
        snap = await reader.snapshot.get("t1", "s1")
        assert snap is not None
        assert snap.status == "done"

    async def test_action_profile(self, tmp_path):
        """ActionSinkAdapter enqueues commands; dispatcher executes them."""
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)
        storage = FakeStorage()
        await storage.initialize_task("t1", "running")
        orch = TaskOrchestrator(storage, governor=None)
        dispatcher = ActionDispatcher(queue, orch, recovery=None)

        await dispatcher.start()
        try:
            receipt = await sink.pause_node("t1")
            assert receipt.accepted is True

            assert await _wait_for(
                lambda: queue._receipts.get(receipt.action_id) is not None
            )

            task = await storage.get_task("t1")
            assert task["cancel_requested"] is True
        finally:
            await dispatcher.stop()


# ---------- End-to-End Governance Loop ----------


class TestGovernanceLoop:
    """End-to-end: bridge → core → storage → UI → action."""

    async def test_full_loop(self, tmp_path):
        """Complete governance loop with all three categories."""
        # 1. Storage adapter
        store = LocalFileStore(tmp_path / "trace")

        # 2. Core (governor + budget)
        governor = FakeGovernor(capacity=2)
        budget = BudgetLedger(
            FakeQuota(), root_task_id="root-1", max_units=100
        )
        await budget.open()

        # 3. Bridge
        async def mock_endpoint(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_chat_response(tokens=30))

        http = httpx.AsyncClient(transport=httpx.MockTransport(mock_endpoint))
        llm = GovernedLLMClient(
            "http://mock",
            governor=governor,
            resource="llm",
            budget=budget,
            audit_writer=store.audit,
            content_writer=store.content,
            model="gpt-4o",
            task_id="task-1",
            http_client=http,
        )

        # 4. Make governed calls
        await llm.chat(
            messages=[{"role": "user", "content": "hello"}],
            call_id="call-1",
        )

        # 5. Verify core governance
        assert governor.acquired == ["llm"]
        assert await budget.balance() == 70  # 100 - 30

        # 6. Verify storage adapter
        events = []
        for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines():
            events.append(json.loads(line))
        assert len(events) == 1
        assert events[0]["data"]["event_id"] == "call-1"

        # 7. Verify UI adapter read
        reader = TraceBundleReader(tmp_path / "trace")
        audit_rows = await reader.audit.query(task_id="task-1")
        assert len(audit_rows) == 1

        # 8. Verify UI adapter action
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue, audit_writer=store.audit)
        receipt = await sink.pause_node("task-1", actor="test-user")
        assert receipt.accepted is True

        # 9. Verify action audit
        action_events = [
            json.loads(line)["data"]
            for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines()
            if json.loads(line)["data"].get("event_type") == "action_pause"
        ]
        assert len(action_events) == 1


# ---------- Swap Tests ----------


class TestSwapTests:
    """Adapters and bridges are interchangeable."""

    async def test_swap_adapter_without_changing_bridge(self, tmp_path):
        """Replace LocalFileStore with MemoryStore, bridge code unchanged."""
        for store_factory in (
            lambda: LocalFileStore(tmp_path / "local"),
            MemoryStore,
        ):
            store = store_factory()
            governor = FakeGovernor()

            async def mock_endpoint(request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json=_chat_response(tokens=10))

            http = httpx.AsyncClient(
                transport=httpx.MockTransport(mock_endpoint)
            )
            llm = GovernedLLMClient(
                "http://mock",
                governor=governor,
                resource="llm",
                audit_writer=store.audit,
                model="gpt-4o",
                http_client=http,
            )

            result = await llm.chat(
                messages=[{"role": "user", "content": "test"}]
            )
            assert result["usage"]["total_tokens"] == 10

            # Both adapters recorded the event
            if hasattr(store.audit, "_events"):
                assert len(store.audit._events) == 1
            else:
                assert (tmp_path / "local" / "audit.ndjson").is_file()


# ---------- Data Rules Validation ----------


class TestDataRules:
    """run_rules validates trace bundles with zero violations."""

    async def test_trace_bundle_rules(self, tmp_path):
        """Trace bundle passes all DR rules."""
        store = LocalFileStore(tmp_path / "trace")

        # Produce some data
        await store.snapshot.save(
            TaskSnapshot(
                task_id="t1", step="s1", execution_id="e1", status="done"
            )
        )
        await store.audit.append(
            AuditEvent(event_id="ev1", task_id="t1", payload={"x": 1})
        )

        # Read all lines
        lines = []
        for name in ("snapshots.ndjson", "audit.ndjson", "deps.ndjson"):
            path = tmp_path / "trace" / name
            if path.is_file():
                for line in path.read_text().splitlines():
                    lines.append(json.loads(line))

        report = run_rules(lines)
        assert report.ok, report.summary()


# ---------- Streaming Test ----------


class TestStreaming:
    """Streaming calls work via LLMSourceProtocol."""

    async def test_streaming_call(self, tmp_path):
        """stream() yields SourceChunk and charges at end."""
        store = LocalFileStore(tmp_path / "trace")

        async def mock_stream(request: httpx.Request) -> httpx.Response:
            lines = [
                json.dumps({"choices": [{"delta": {"content": "he"}}]}),
                json.dumps({"choices": [{"delta": {"content": "llo"}}]}),
                json.dumps({
                    "model": "gpt-4o",
                    "choices": [{"delta": {}}],
                    "usage": {"total_tokens": 5, "prompt_tokens": 2, "completion_tokens": 3},
                }),
            ]
            text = "".join(f"data: {l}\n\n" for l in lines) + "data: [DONE]\n\n"
            return httpx.Response(
                200, text=text, headers={"Content-Type": "text/event-stream"}
            )

        http = httpx.AsyncClient(transport=httpx.MockTransport(mock_stream))
        llm = GovernedLLMClient(
            "http://mock",
            governor=FakeGovernor(),
            resource="llm",
            audit_writer=store.audit,
            model="gpt-4o",
            http_client=http,
        )

        chunks = []
        async for chunk in llm.stream(
            messages=[{"role": "user", "content": "hi"}]
        ):
            chunks.append(chunk)

        texts = [c.text for c in chunks if c.text]
        assert texts == ["he", "llo"]
        assert chunks[-1].finish is True

        # Audit event written at stream end
        events = []
        for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines():
            events.append(json.loads(line))
        assert len(events) == 1

# ---------- Formal profile certification (sync: run_conformance uses asyncio.run) ----------


class TestBridgeOpenAIProducerProfile:
    """bridge-openai passes the producer conformance profile (freeze criterion).

    NOTE: run_conformance internally calls asyncio.run(), so these tests
    must be sync functions.
    """

    def test_bridge_audit_sink_producer(self, tmp_path):
        """The bridge's audit write path (GovernedLLMClient -> protocol sink)
        satisfies the producer profile against a real audit adapter.

        Producer profile semantics: sink half-domains verified as declared;
        query not required. We drive the bridge to produce real audit events
        into LocalFileStore's audit part, then certify that part.
        """
        store = LocalFileStore(tmp_path / "trace")

        async def mock_endpoint(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=_chat_response(tokens=10))

        http = httpx.AsyncClient(transport=httpx.MockTransport(mock_endpoint))
        llm = GovernedLLMClient(
            "http://mock",
            governor=FakeGovernor(),
            resource="llm",
            audit_writer=store.audit,
            model="gpt-4o",
            http_client=http,
        )
        asyncio.run(
            llm.chat(messages=[{"role": "user", "content": "hi"}], call_id="p-1")
        )

        # The audit part that the bridge wrote into is a producer-tier sink.
        report = run_conformance(store.audit, profile="producer")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

        # And the bridge's event actually landed with the call_id idempotency key.
        assert (tmp_path / "trace" / "audit.ndjson").is_file()
        lines = (tmp_path / "trace" / "audit.ndjson").read_text().splitlines()
        assert any(json.loads(l)["data"].get("event_id") == "p-1" for l in lines)


class TestAdapterUIConsumerProfile:
    """adapter-ui passes the consumer conformance profile (freeze criterion).

    NOTE: run_conformance internally calls asyncio.run(), so these tests
    must be sync functions.
    """

    def test_reader_consumer_profile(self, tmp_path):
        """TraceBundleReader's snapshot view passes consumer profile.

        The reader implements seed() (conformance consumer hook), so the
        CF-VIEW seeded cases run rather than degrade to skip.
        """
        reader = TraceBundleReader(tmp_path / "empty-bundle")
        report = run_conformance(reader.snapshot, profile="consumer")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()

        # Seed hook implemented -> CF-VIEW cases executed (not degraded skip)
        view_results = [r for r in report.results if r.half_domain == "view"]
        assert view_results, "expected CF-VIEW seeded cases to be present"
        assert all(r.status == "passed" for r in view_results), report.summary()

    def test_reader_dependency_consumer_profile(self, tmp_path):
        """TraceBundleReader's dependency view passes consumer profile."""
        reader = TraceBundleReader(tmp_path / "empty-bundle")
        report = run_conformance(reader.dependency, profile="consumer")
        assert report.eligibility_error is None
        assert report.failed == 0, report.summary()