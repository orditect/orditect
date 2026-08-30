"""GovernedLLMClient: OpenAI-compatible endpoint bridge (endpoint tier).

Wraps any OpenAI-compatible chat-completions endpoint in the governed-call
form (semaphore + budget + audit + content pointer-ization), exposing two
forms with one client:

- chat(): non-streaming call; returns the endpoint's result dict.
- stream(): streaming call; yields orditect-stream SourceChunk objects,
  implementing LLMSourceProtocol for direct use with StreamRunner.

Boundary discipline (bridge-discipline.md):
- OpenAI-shaped vocabulary (model / messages / usage / finish_reason) is
  translated at THIS package's edge and never flows back into framework
  packages: into audit payloads (opaque dicts) and cost dicts only.
- Vocabulary declaration: event_type words and payload keys used here are
  documented in README (bridge-side projection of T6).
- Clock duty (T7): timestamps in audit events are produced by this bridge's
  own timezone-aware UTC clock (protocol model default).

cost_fn discipline (A5): usage missing (e.g. a stream without
stream_options.include_usage) -> cost_fn receives None; the business prices
it. This bridge never silently estimates.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx

from orditect.flow.governor.call import GovernedCallClient
from orditect.flow.protocols.governor import ResourceGovernorProtocol
from orditect.stream.protocols.source import SourceChunk, SourceRequest

logger = logging.getLogger(__name__)

#: Default cost: charge total tokens when usage is present; 0 when usage is
#: absent (usage=None means "the endpoint did not report"; business may
#: override cost_fn to price by call, latency, or flat rate).
def _tokens_cost(result: Any) -> int:
    if not isinstance(result, dict):
        return 0
    usage = result.get("usage") or {}
    return int(usage.get("total_tokens", 0))


class GovernedLLMClient:
    """Governed OpenAI-compatible LLM client (bridge reference).

    Args:
        base_url: OpenAI-compatible endpoint root (e.g.
            "https://api.openai.com" or "http://localhost:11434/v1").
        api_key: Bearer token (may be None for local endpoints).
        governor: Resource governance instance (semaphore routing).
        resource: Resource name for sem/quota (e.g. "llm").
        budget: BudgetLedger (optional). Pre-check + post-charge via cost_fn.
        cost_fn: result -> units. Default: usage.total_tokens (0 when usage
            absent). Receives None for usage-missing streams.
        audit_writer: Protocol AuditWriter (optional).
        content_writer: Protocol ContentWriter (optional). Large bodies
            (messages / long thinking / tool JSON) are pointer-ized when
            present and content_writer is set.
        model: Default model for requests (caller vocabulary).
        task_id / parent_task_id / execution_id: Opaque labels (optional).
        timeout: HTTP timeout seconds.
        http_client: Optional injected httpx.AsyncClient (testing / pooling).
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        governor: ResourceGovernorProtocol,
        resource: str,
        budget: Any = None,
        cost_fn: Any = None,
        audit_writer: Any = None,
        content_writer: Any = None,
        model: str | None = None,
        task_id: str | None = None,
        parent_task_id: str | None = None,
        execution_id: str | None = None,
        timeout: float = 60.0,
        http_client: httpx.AsyncClient | None = None,
    ):
        self._base = base_url.rstrip("/")
        self._api_key = api_key
        self._model = model
        self._http = http_client or httpx.AsyncClient(timeout=timeout)
        self._owns_http = http_client is None
        self._call = GovernedCallClient(
            governor,
            resource,
            budget=budget,
            cost_fn=cost_fn or _tokens_cost,
            audit_writer=audit_writer,
            content_writer=content_writer,
            event_type="llm_call",
            task_id=task_id,
            parent_task_id=parent_task_id,
            execution_id=execution_id,
            content_type="application/json",
        )

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    # ---------- non-streaming ----------

    async def chat(
        self,
        *,
        messages: list[dict] | None = None,
        model: str | None = None,
        call_id: str | None = None,
        **kwargs,
    ) -> dict:
        """Non-streaming governed call. Returns the endpoint result dict."""
        payload = self._payload(messages=messages, model=model, **kwargs)

        async def _do() -> dict:
            resp = await self._http.post(
                f"{self._base}/chat/completions",
                json=payload,
                headers=self._headers(),
            )
            resp.raise_for_status()
            # C5 (v0.1.5): latency is recorded by GovernedCallClient as
            # elapsed_ms; never leak internal fields into the caller-visible
            # provider response.
            return resp.json()

        return await self._call.call(
            handler=_do,
            call_id=call_id,
            payload_fn=self._audit_payload,
            content_fn=self._content_bytes(messages),
        )

    # ---------- streaming (LLMSourceProtocol) ----------

    async def stream(
            self,
            request: SourceRequest | None = None,
            *,
            messages: list[dict] | None = None,
            model: str | None = None,
            cancel_token: Any = None,
            call_id: str | None = None,
            **kwargs,
    ) -> AsyncIterator[SourceChunk]:
        """Streaming governed call implementing LLMSourceProtocol.

        Accepts either a SourceRequest (orditect-stream entry) or explicit
        messages= kwargs (direct use). Yields SourceChunk(text=...) deltas and
        a final SourceChunk(finish=True).

        Closing discipline (v0.1.7): closing this generator deterministically
        acloses the governed stream (whose own finally closes the HTTP stream
        and releases the semaphore) — resource cleanup never relies on GC
        timing.
        """
        if request is not None:
            payload = dict(request.payload)
        else:
            payload = {}
        if messages is not None:
            payload["messages"] = messages
        if model is not None:
            payload["model"] = model
        payload.update(kwargs)
        body = self._payload(stream=True, **payload)

        result_holder: dict[str, Any] = {}
        partial: list[str] = []

        async def _gen():
            started = time.monotonic()
            async with self._http.stream(
                    "POST",
                    f"{self._base}/chat/completions",
                    json=body,
                    headers=self._headers(),
            ) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        obj = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    choices = obj.get("choices") or []
                    if choices:
                        delta = choices[0].get("delta") or {}
                        text = delta.get("content")
                        if text:
                            partial.append(text)
                            yield SourceChunk(text=text)
                    if obj.get("usage"):
                        result_holder["usage"] = obj["usage"]
                        result_holder["model"] = obj.get("model")
            # C5 (v0.1.5 / v0.1.6): latency is recorded by GovernedCallClient
            # as elapsed_ms. The result holder carries only endpoint
            # vocabulary — never internal fields (_latency_ms would leak into
            # the caller-visible provider response and into cost_fn input).
            yield SourceChunk(finish=True)

        governed_stream = self._call.call_streaming(
            handler=_gen,
            cancel_token=cancel_token,
            call_id=call_id,
            result_fn=lambda: (
                result_holder
                if result_holder.get("usage") is not None
                else None
            ),
            partial_fn=lambda: "".join(partial).encode("utf-8") or None,
            payload_fn=lambda r: self._audit_payload(r),
        )
        try:
            async for chunk in governed_stream:
                yield chunk
        finally:
            # v0.1.7: aclose cascade — async-for never acloses inner
            # iterators, so the governed stream must be closed explicitly.
            aclose = getattr(governed_stream, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except RuntimeError:
                    pass

    # ---------- internals ----------

    def _payload(self, stream: bool = False, **kwargs) -> dict:
        body: dict[str, Any] = {
            k: v for k, v in kwargs.items() if v is not None
        }
        if self._model and "model" not in body:
            body["model"] = self._model
        if stream:
            body["stream"] = True
            body.setdefault("stream_options", {"include_usage": True})
        return body

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def _audit_payload(self, result: Any) -> dict:
        """Translate endpoint vocabulary into the audit payload (edge)."""
        if not isinstance(result, dict):
            return {}
        out: dict[str, Any] = {}
        if result.get("model"):
            out["model"] = result["model"]
        usage = result.get("usage")
        if isinstance(usage, dict):
            out["usage"] = usage
        choices = result.get("choices")
        if isinstance(choices, list) and choices:
            finish = choices[0].get("finish_reason")
            if finish:
                out["finish_reason"] = finish
        return out

    def _content_bytes(self, messages: list[dict] | None):
        """Pointer-ize the request messages when a content writer is set."""

        def fn(result: Any) -> bytes | None:
            if not messages:
                return None
            return json.dumps(messages, ensure_ascii=False).encode("utf-8")

        return fn