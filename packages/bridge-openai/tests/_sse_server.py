"""In-process fake OpenAI-compatible SSE server for termination tests.

Serves canned SSE bodies over a real httpx ASGI transport (no sockets),
captures the request bodies, and fakes the governance plane (governor,
audit writer) so GovernedLLMClient runs end to end.
"""
from __future__ import annotations

import json
from typing import Any

import httpx


class _FakeGovernor:
    """Unbounded governor: acquire always succeeds."""

    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        self.acquired.append(resource)
        return f"token-{len(self.acquired)}"

    async def try_acquire(self, resource: str) -> str | None:
        return await self.acquire(resource)

    async def release(self, resource: str, token: str) -> None:
        self.released.append(token)

    async def get_usage(self, resource: str) -> int:
        return len(self.acquired) - len(self.released)


class _FakeAuditWriter:
    """Captures AuditEvent objects; .events exposes their payloads.

    The flow-side GovernedCallClient writes via append(AuditEvent(...))
    (protocol AuditWriter); payloads are what the tests assert against.
    """

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def append(self, event: Any) -> None:
        self.records.append(event)

    @property
    def events(self) -> list[dict]:
        return [getattr(e, "payload", e) for e in self.records]


def frames_to_body(*frames: str) -> str:  # re-exported for tests
    return "".join(frames)


class FakeSSEServer:
    """ASGI app serving one canned SSE body per POST."""

    def __init__(self, body: str, *, status: int = 200) -> None:
        self._body = body
        self._status = status
        self.governor = _FakeGovernor()
        self.requests: list[dict] = []
        self._audit_writer = _FakeAuditWriter()
        self.http_client: httpx.AsyncClient | None = None

    @property
    def base_url(self) -> str:
        return "http://fake-llm.test/v1"

    @property
    def audits(self) -> list[dict]:
        return self._audit_writer.events

    @property
    def last_audit(self) -> dict:
        assert self.audits, "no audit events captured"
        return self.audits[-1]

    @property
    def audit_writer(self) -> _FakeAuditWriter:
        """Constructor-time injection point for GovernedLLMClient."""
        return self._audit_writer

    async def _app(self, scope, receive, send) -> None:
        assert scope["type"] == "http"
        body = b""
        while True:
            message = await receive()
            if message["type"] == "http.request":
                body += message.get("body", b"")
                if not message.get("more_body"):
                    break
        try:
            self.requests.append(json.loads(body.decode() or "{}"))
        except json.JSONDecodeError:
            self.requests.append({})

        payload = self._body.encode()
        await send({
            "type": "http.response.start",
            "status": self._status,
            "headers": [(b"content-type", b"text/event-stream")],
        })
        await send({
            "type": "http.response.body",
            "body": payload,
            "more_body": False,
        })

    async def __aenter__(self) -> "FakeSSEServer":
        transport = httpx.ASGITransport(app=self._app)
        self.http_client = httpx.AsyncClient(
            transport=transport, timeout=30.0)
        return self

    async def __aexit__(self, *exc) -> None:
        if self.http_client is not None:
            await self.http_client.aclose()