"""End-to-end governance loop verification (v0.1.3 dogfood).

Validates the full three-category integration:
bridge (GovernedLLMClient) → core (semaphore/budget) → storage adapter
(LocalFileStore) → UI adapter (TraceBundleReader + ActionSinkAdapter) →
flow (ActionDispatcher).

This test uses in-memory infrastructure (no external services) to prove
the integration pattern works.
"""

import asyncio
import json

import httpx
import pytest

from orditect.adapter.local import LocalFileStore
from orditect.adapter.ui import (
    ActionSinkAdapter,
    MemoryActionQueue,
    TraceBundleReader,
)
from orditect.bridge.openai import GovernedLLMClient
from orditect.flow import (
    BaseBackEndTask,
    BudgetLedger,
    RecoveryService,
    TaskOrchestrator,
)
from orditect.flow.actions import ActionDispatcher
from orditect.flow.governor.factory import TaskbaseGovernorAdapter
from orditect.protocol.rules import run_rules

pytestmark = pytest.mark.integration


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


def _chat_response(tokens: int):
    return {
        "id": "chatcmpl-1",
        "model": "gpt-4o",
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


class TestGovernanceLoop:
    async def test_full_loop(self, tmp_path):
        """Full integration: bridge → core → storage → UI → action."""
        # 1. Setup storage adapter (trace bundle producer)
        store = LocalFileStore(tmp_path / "trace")

        # 2. Setup core (governor + budget)
        governor = FakeGovernor(capacity=2)

        class FakeQuota:
            def __init__(self):
                self._pending = 0

            async def reserve_units(self, **kw):
                if self._pending + kw["units"] > kw["max_units"]:
                    return {"ok": False, "reason": "limit_exceeded"}
                self._pending += kw["units"]
                return {"ok": True}

            async def get_pending_units(self, **kw):
                return self._pending

        budget = BudgetLedger(
            FakeQuota(), root_task_id="root-1", max_units=100
        )
        await budget.open()

        # 3. Setup bridge (GovernedLLMClient with mock endpoint)
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

        # 4. Make governed calls (semaphore + budget + audit + content)
        result1 = await llm.chat(
            messages=[{"role": "user", "content": "hello"}],
            call_id="call-1",
        )
        result2 = await llm.chat(
            messages=[{"role": "user", "content": "world"}],
            call_id="call-2",
        )

        # 5. Verify core governance worked
        assert governor.acquired == ["llm", "llm"]
        assert governor.released == ["llm", "llm"]
        assert await budget.balance() == 40  # 100 - 60 (30+30 tokens)

        # 6. Verify storage adapter persisted data
        events = []
        for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines():
            events.append(json.loads(line))
        assert len(events) == 2
        assert events[0]["data"]["event_id"] == "call-1"
        assert events[1]["data"]["event_id"] == "call-2"
        assert events[0]["data"]["task_id"] == "task-1"

        # 7. Verify UI adapter can read the trace bundle
        reader = TraceBundleReader(tmp_path / "trace")
        audit_rows = await reader.audit.query(task_id="task-1")
        assert len(audit_rows) == 2
        assert audit_rows[0].payload["usage"]["total_tokens"] == 30

        # 8. Verify UI adapter can drive actions (command-queue form)
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue, audit_writer=store.audit)

        receipt = await sink.pause_node("task-1", actor="test-user")
        assert receipt.accepted is True

        command = await queue.dequeue(timeout=0.1)
        assert command is not None
        assert command.target_task_id == "task-1"
        assert command.actor == "test-user"

        # 9. Verify action audit event was written
        action_events = []
        for line in (tmp_path / "trace" / "audit.ndjson").read_text().splitlines():
            data = json.loads(line)["data"]
            if data.get("event_type") == "action_pause":
                action_events.append(data)
        assert len(action_events) == 1
        assert action_events[0]["event_id"] == receipt.action_id

        # 10. Verify run_rules validates the trace bundle with zero violations
        all_lines = []
        for name in ("audit.ndjson", "snapshots.ndjson", "deps.ndjson"):
            path = tmp_path / "trace" / name
            if path.is_file():
                for line in path.read_text().splitlines():
                    all_lines.append(json.loads(line))

        report = run_rules(all_lines)
        assert report.ok, report.summary()

    async def test_swap_adapter_without_changing_bridge(self, tmp_path):
        """Swap test: replace LocalFileStore with MemoryStore, bridge unchanged."""
        from orditect.adapter.memory import MemoryStore

        # Same bridge code, different adapter
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

            # Both adapters recorded the audit event
            if hasattr(store.audit, "_events"):
                # MemoryStore
                assert len(store.audit._events) == 1
            else:
                # LocalFileStore
                audit_file = (
                    tmp_path / "local" / "audit.ndjson"
                    if isinstance(store, LocalFileStore)
                    else None
                )
                assert audit_file is not None and audit_file.is_file()

    async def test_action_dispatcher_executes_pause(self, tmp_path):
        """Verify ActionDispatcher consumes queue and executes pause."""
        from orditect.flow import TaskOrchestrator

        # Setup a minimal task
        class FakeStorage:
            def __init__(self):
                self._tasks = {}

            async def initialize_task(self, task_id, initial_status, **kw):
                self._tasks[task_id] = {
                    "task_id": task_id,
                    "status": initial_status,
                    "cancel_requested": False,
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
                return []

        storage = FakeStorage()
        await storage.initialize_task("task-1", "running")
        orchestrator = TaskOrchestrator(storage, governor=None)

        # Setup action queue and dispatcher
        queue = MemoryActionQueue()
        sink = ActionSinkAdapter(queue)
        dispatcher = ActionDispatcher(queue, orchestrator, recovery=None)

        await dispatcher.start()
        try:
            # UI writes action command
            receipt = await sink.pause_node("task-1")
            assert receipt.accepted is True

            # Wait for dispatcher to execute
            for _ in range(50):
                if queue._receipts.get(receipt.action_id):
                    break
                await asyncio.sleep(0.02)

            exec_receipt = queue._receipts[receipt.action_id]
            assert exec_receipt["status"] == "executed"

            # Verify task was cancelled
            task = await storage.get_task("task-1")
            assert task["cancel_requested"] is True
        finally:
            await dispatcher.stop()