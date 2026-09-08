"""Live endpoint tests against the real LLM configured in .env.

These tests hit the actual OpenAI-compatible endpoint (dashscope / ollama /
whatever .env points at) and pin the termination classification and audit
evidence against real wire behavior. They are skipped automatically when
no endpoint is configured, so CI without credentials stays green.

Required .env keys (package root .env or repo root .env):
    LLM_BASE_URL   e.g. https://dashscope.aliyuncs.com/compatible-mode/v1
    LLM_API_KEY
    LLM_PUBLISH_MODEL (fallback: LLM_RESEARCH_MODEL, LLM_WRITING_MODEL)

Run:
    pytest tests/test_stream_live.py -v -m live
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from orditect.bridge.openai import GovernedLLMClient

pytestmark = pytest.mark.live

# .env discovery: explicit LLM_ENV_FILE wins; otherwise probe the usual
# locations (package root, repo root, CWD).
_ENV_CANDIDATES = []
if os.getenv("LLM_ENV_FILE"):
    _ENV_CANDIDATES.append(Path(os.environ["LLM_ENV_FILE"]))
_here = Path(__file__).resolve()
_ENV_CANDIDATES += [
    _here.parents[1] / ".env",   # package root (bridge-openai/.env)
    _here.parents[2] / ".env",   # packages/ root
    _here.parents[3] / ".env",   # repo root
    Path.cwd() / ".env",
]
for _p in _ENV_CANDIDATES:
    if _p.is_file():
        load_dotenv(_p)
        break

_BASE_URL = os.getenv("LLM_BASE_URL")
_API_KEY = os.getenv("LLM_API_KEY")
_MODEL = (os.getenv("LLM_PUBLISH_MODEL")
          or os.getenv("LLM_RESEARCH_MODEL")
          or os.getenv("LLM_WRITING_MODEL"))

requires_endpoint = pytest.mark.skipif(
    not _BASE_URL or not _MODEL,
    reason="no live LLM endpoint configured in .env",
)


class _FakeGovernor:
    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        return "live-token"

    async def try_acquire(self, resource: str) -> str | None:
        return "live-token"

    async def release(self, resource: str, token: str) -> None:
        return None

    async def get_usage(self, resource: str) -> int:
        return 0


class _ListAuditWriter:
    """Captures AuditEvent objects via the flow-side append() contract;
    .events exposes their payloads for assertions."""

    def __init__(self) -> None:
        self.records: list = []

    async def append(self, event) -> None:
        self.records.append(event)

    @property
    def events(self) -> list[dict]:
        return [getattr(e, "payload", e) for e in self.records]


def _live_client(audit: _ListAuditWriter, **kwargs) -> GovernedLLMClient:
    return GovernedLLMClient(
        _BASE_URL,
        api_key=_API_KEY,
        governor=_FakeGovernor(),
        resource="llm",
        audit_writer=audit,
        model=_MODEL,
        timeout=180.0,
        **kwargs,
    )


@requires_endpoint
class TestLiveStreamTermination:
    @pytest.mark.asyncio
    async def test_real_stream_completes_with_full_tail(self):
        """The real endpoint's stream completes; the tail is not dropped."""
        audit = _ListAuditWriter()
        client = _live_client(audit)
        chunks = []
        async for chunk in client.stream(
            messages=[{
                "role": "user",
                "content": (
                    "Write exactly three short paragraphs about EV battery "
                    "risks. End the final paragraph with the word DONE."
                ),
            }],
        ):
            chunks.append(chunk)
        await client.aclose()

        text = "".join(c.text or "" for c in chunks)
        assert text, "live endpoint returned an empty body"
        assert chunks[-1].finish is True, (
            "protocol finish chunk missing — tail flush would not fire")
        assert text.rstrip().endswith("DONE") or len(text) > 200, (
            f"tail appears truncated: ...{text[-120:]!r}")

        assert audit.events, "no audit event written for the stream"
        payload = audit.events[-1]
        assert payload.get("termination") == "completed", (
            f"unexpected termination: {payload}")
        assert payload.get("stream_chunks", 0) > 0

    @pytest.mark.asyncio
    async def test_real_stream_usage_and_finish_reason_recorded(self):
        """usage (when the endpoint reports it) and finish_reason land
        in the audit payload — the empty-choices tail chunk regression."""
        audit = _ListAuditWriter()
        client = _live_client(audit)
        async for _ in client.stream(
            messages=[{"role": "user", "content": "Say OK."}],
            include_usage=True,
        ):
            pass
        await client.aclose()

        payload = audit.events[-1]
        assert payload.get("finish_reason") in ("stop", "length", None), (
            f"unexpected finish_reason: {payload.get('finish_reason')!r}")
        usage = payload.get("usage")
        if usage is not None:
            assert usage.get("total_tokens", 0) > 0
        # usage absent is legitimate (A5): the endpoint may not report it;
        # the pin only guarantees no crash and a completed termination.
        assert payload.get("termination") == "completed"

    @pytest.mark.asyncio
    async def test_max_tokens_length_truncation_is_visible(self):
        """finish_reason='length' stays protocol-completed and auditable."""
        audit = _ListAuditWriter()
        client = _live_client(audit)
        chunks = []
        async for chunk in client.stream(
            messages=[{
                "role": "user",
                "content": "Write a very long essay about the EV industry.",
            }],
            max_tokens=16,
        ):
            chunks.append(chunk)
        await client.aclose()

        payload = audit.events[-1]
        assert payload.get("termination") == "completed"
        # OpenAI-compatible endpoints signal max_tokens via finish_reason;
        # some omit it — then at least the completed classification holds.
        if payload.get("finish_reason") is not None:
            assert payload["finish_reason"] == "length", (
                f"expected length truncation, got {payload}")