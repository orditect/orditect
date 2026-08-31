"""Workflow tasks for the stream demo.

CollectTask / ReportTask are unchanged from the real-world demo.
StreamAnalyzeTask is the output-plane showcase: inside the task, a
StreamRunner turns the governed LLM stream into a protocol SSE event
sequence (the product output for end users), while the task itself
remains a flow-governed node.
"""

from __future__ import annotations

import asyncio

from orditect.flow import BaseBackEndTask, TaskOrchestrator
from orditect.stream import (
    DEFAULT_CONFIG,
    EnrichMode,
    MockVectorEnricher,
    SourceChunk,
    SourceRequest,
    StageConfig,
    SourceType,
    StreamRunner,
)
from orditect.stream.store import get_protocol_store

ROOT_TASK_ID = "pipeline-root"

# Prompt engineered to make the model emit the marker so the enrich
# placeholder pipeline triggers (instruct the model explicitly).
_ANALYZE_PROMPT = (
    "Analyze the collected docs in 2 short paragraphs. "
    "After the first paragraph, output exactly the token ![img] on its own, "
    "then continue with the second paragraph."
)


class CollectTask(BaseBackEndTask):
    """Step 1: collect documents (pure local work)."""

    async def execute(self, task_id: str, **kwargs) -> dict:
        await asyncio.sleep(0.2)
        return {"docs": ["doc-alpha", "doc-beta"]}


class _GovernedSource:
    """LLM source that streams via raw httpx, with explicit governance.

    Streams through GovernedCallClient.call_streaming for sem/budget/audit.
    The cancel_token is handled defensively (orditect's CancellationToken
    is async; a duck-typed sync token would break a bare await).
    """

    def __init__(self, llm) -> None:
        self._llm = llm

    @staticmethod
    async def _is_cancelled(token) -> bool:
        if token is None:
            return False
        fn = getattr(token, "is_cancelled", None)
        if fn is None:
            return False
        import inspect
        result = fn()
        if inspect.isawaitable(result):
            return bool(await result)
        return bool(result)

    async def stream(self, request: SourceRequest, cancel_token=None):
        import json as _json
        import httpx as _httpx

        payload = {
            "model": self._llm._model,
            "messages": request.payload["messages"],
            "stream": True,
        }
        headers = {"Content-Type": "application/json"}
        if self._llm._api_key:
            headers["Authorization"] = f"Bearer {self._llm._api_key}"

        async def _gen():
            async with _httpx.AsyncClient(timeout=120.0) as client:
                async with client.stream(
                    "POST",
                    f"{self._llm._base}/chat/completions",
                    headers=headers,
                    json=payload,
                ) as resp:
                    resp.raise_for_status()
                    async for line in resp.aiter_lines():
                        if await self._is_cancelled(cancel_token):
                            continue
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if data == "[DONE]":
                            break
                        try:
                            obj = _json.loads(data)
                        except _json.JSONDecodeError:
                            continue
                        choices = obj.get("choices") or []
                        if choices:
                            text = (choices[0].get("delta") or {}).get("content")
                            if text:
                                yield SourceChunk(text=text)
            yield SourceChunk(finish=True)

        governed = self._llm._call.call_streaming(
            handler=_gen,
            cancel_token=None,  # cancel handled inside _gen (defensively)
            call_id=request.payload.get("call_id"),
            result_fn=lambda: None,
            partial_fn=None,
        )
        try:
            async for chunk in governed:
                yield chunk
        finally:
            aclose = getattr(governed, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except RuntimeError:
                    pass


class StreamAnalyzeTask(BaseBackEndTask):
    """Step 2: governed LLM call, streamed to end users as protocol SSE.

    Inside one governed node we run a StreamRunner: the LLM's streaming
    output becomes stream.delta / enrich.* / stage.end / stream.manifest
    events. The runner's aggregated content is returned as the task result
    (so the snapshot/result chain stays intact), while the live event
    stream is printed for the demo.
    """

    def __init__(self, storage, llm, store, on_event=None) -> None:
        super().__init__(storage)
        self._llm = llm
        self._store = store
        self._on_event = on_event  # async callback(envelope, event_type)

    async def execute(self, task_id: str, **kwargs) -> dict:
        record = await self.storage.get_task(task_id)
        eid = record.get("execution_id", "")
        call_id = f"analyze-{task_id}-{eid}"

        source = _GovernedSource(self._llm)
        result_store = get_protocol_store(self._store.result, self._store.result)
        runner = StreamRunner(
            stages=[
                StageConfig(
                    name="analyze",
                    source_type=SourceType.LLM,
                    source=source,
                    request=SourceRequest(payload={
                        "messages": [{"role": "user", "content": _ANALYZE_PROMPT}],
                        "call_id": call_id,
                    }),
                ),
            ],
            enricher=MockVectorEnricher(latency=0.05),
            store=result_store,
            config=DEFAULT_CONFIG.merge(
                enrich_mode=EnrichMode.LOCAL, enrich_settle_timeout=1.0
            ),
            loading_url="https://oss.example.com/loading.jpg",
        )
        import sys as _sys
        collected_text: list[str] = []

        async for envelope, event_type in runner.run():
            if self._on_event is not None:
                await self._on_event(envelope, event_type)
            if event_type.value == "stream.delta" and envelope.data.get("kind") == "content":
                collected_text.append(envelope.data.get("text", ""))
        return {"analysis": "".join(collected_text)}


class ReportTask(BaseBackEndTask):
    """Step 3: unchanged from the real-world demo (non-streaming, fails
    on first generation for the HITL retry path)."""

    def __init__(self, storage, llm, fail_flags: dict) -> None:
        super().__init__(storage)
        self._llm = llm
        self._fail_flags = fail_flags

    async def execute(self, task_id: str, **kwargs) -> dict:
        if self._fail_flags.pop(task_id, False):
            raise RuntimeError("simulated first-run failure (retry via HITL)")
        record = await self.storage.get_task(task_id)
        eid = record.get("execution_id", "")
        result = await self._llm.chat(
            messages=[{"role": "user", "content": "Write the final report"}],
            call_id=f"report-{task_id}-{eid}",
        )
        return {"report": result["choices"][0]["message"]["content"]}


class PipelineTask(BaseBackEndTask):
    """Root task: collect -> analyze(streamed) -> report."""

    def __init__(self, storage, orchestrator: TaskOrchestrator, llm, store,
                 fail_flags, on_event=None, step_timeout: float = 300.0) -> None:
        super().__init__(storage)
        self._orchestrator = orchestrator
        self._llm = llm
        self._store = store
        self._fail_flags = fail_flags
        self._on_event = on_event
        self._step_timeout = step_timeout

    async def execute(self, task_id: str, **kwargs) -> dict:
        collect = await self._orchestrator.submit(
            CollectTask(self.storage), task_id="collect"
        )
        await self._orchestrator.wait_terminal(collect, timeout=self._step_timeout)

        analyze = await self._orchestrator.submit(
            StreamAnalyzeTask(self.storage, self._llm, self._store, self._on_event),
            task_id="analyze",
        )
        await self._orchestrator.wait_terminal(analyze, timeout=self._step_timeout)

        report = await self._orchestrator.submit(
            ReportTask(self.storage, self._llm, self._fail_flags),
            task_id="report",
        )
        record = await self._orchestrator.wait_terminal(report, timeout=self._step_timeout)
        return {"report_status": record["status"]}


def make_task_factory(storage, llm, store, fail_flags, orchestrator=None, on_event=None):
    """Build the task_factory required by RecoveryService."""

    async def factory(task_id: str):
        if task_id == ROOT_TASK_ID:
            if orchestrator is None:
                raise KeyError("pipeline-root needs an orchestrator to rebuild")
            return PipelineTask(storage, orchestrator, llm, store, fail_flags, on_event)
        if task_id == "collect":
            return CollectTask(storage)
        if task_id == "analyze":
            return StreamAnalyzeTask(storage, llm, store, on_event)
        if task_id == "report":
            return ReportTask(storage, llm, fail_flags)
        raise KeyError(f"unknown task_id: {task_id}")

    return factory