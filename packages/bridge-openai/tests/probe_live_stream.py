"""Live probe: print the REAL LLM stream through the fixed GovernedLLMClient.

Drives client.stream() directly against the endpoint configured in
package-root .env, printing every chunk as it arrives with channel tags
(content / thinking / finish), then dumps the audit evidence written for
the call (termination class, finish_reason, chunk count, usage).

Cancel beat: --cancel-after N flips the cancel token after N chunks. The
output stops printing, but the governed stream keeps being consumed to
its terminal point (drain) — the gap between the cancel timestamp and
the final elapsed time is the drain window, and the usage figure covers
the tokens generated during it.

Run from the package root:
    python -m tests.probe_live_stream
    python -m tests.probe_live_stream "Your custom prompt here"
    python -m tests.probe_live_stream --max-tokens 24 "long essay prompt"
    python -m tests.probe_live_stream --cancel-after 30
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from orditect.bridge.openai import GovernedLLMClient

_ENV = Path(__file__).resolve().parents[1] / ".env"
if _ENV.is_file():
    load_dotenv(_ENV)

_BASE_URL = os.getenv("LLM_BASE_URL")
_API_KEY = os.getenv("LLM_API_KEY")
_MODEL = (os.getenv("LLM_PUBLISH_MODEL")
          or os.getenv("LLM_RESEARCH_MODEL")
          or os.getenv("LLM_WRITING_MODEL"))

DEFAULT_PROMPT = (
    "Write exactly three short paragraphs about the main risks of the EV "
    "industry. After the first paragraph, output the token ![img] on its "
    "own line, then continue. End the final paragraph with the word DONE."
)


class _ProbeGovernor:
    """Unbounded governor for the probe (no redis)."""

    async def acquire(self, resource: str, timeout: float | None = None) -> str:
        return "probe-token"

    async def try_acquire(self, resource: str) -> str | None:
        return "probe-token"

    async def release(self, resource: str, token: str) -> None:
        return None

    async def get_usage(self, resource: str) -> int:
        return 0


class _ProbeAuditWriter:
    """Captures the single AuditEvent written at stream close."""

    def __init__(self) -> None:
        self.records: list = []

    async def append(self, event) -> None:
        self.records.append(event)

    @property
    def last_payload(self) -> dict:
        if not self.records:
            return {}
        return getattr(self.records[-1], "payload", {})


class _ProbeToken:
    """Cancel token flipped by the probe's cancel beat."""

    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    async def is_cancelled(self) -> bool:
        return self._cancelled


async def main() -> int:
    if not _BASE_URL or not _MODEL:
        print("ERROR: LLM_BASE_URL / model not configured in .env",
              file=sys.stderr)
        return 2

    argv = sys.argv[1:]
    max_tokens: int | None = None
    cancel_after: int | None = None
    while argv and argv[0].startswith("--"):
        if argv[0] == "--max-tokens":
            max_tokens = int(argv[1])
            argv = argv[2:]
        elif argv[0] == "--cancel-after":
            cancel_after = int(argv[1])
            argv = argv[2:]
        else:
            break
    prompt = argv[0] if argv else DEFAULT_PROMPT

    audit = _ProbeAuditWriter()
    client = GovernedLLMClient(
        _BASE_URL,
        api_key=_API_KEY,
        governor=_ProbeGovernor(),
        resource="llm",
        audit_writer=audit,
        model=_MODEL,
        timeout=180.0,
    )

    kwargs: dict = {}
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens

    token = _ProbeToken()

    print(f"== endpoint: {_BASE_URL}  model: {_MODEL}")
    if max_tokens is not None:
        print(f"== max_tokens: {max_tokens} (expect finish_reason=length)")
    if cancel_after is not None:
        print(f"== cancel beat: after chunk #{cancel_after} "
              f"(output stops, the stream keeps being consumed)")
    print("== stream begin " + "=" * 50)

    n_content = n_thinking = 0
    content_chars = thinking_chars = 0
    finish_seen = False
    cancelled_at: float | None = None
    t0 = time.monotonic()
    try:
        async for chunk in client.stream(
            messages=[{"role": "user", "content": prompt}],
            call_id="probe-live-stream-1",
            cancel_token=token,
            **kwargs,
        ):
            if (cancel_after is not None and not token._cancelled
                    and n_content + n_thinking >= cancel_after):
                token.cancel()
                cancelled_at = time.monotonic() - t0
                print(f"\n== [CANCEL at {cancelled_at:.1f}s; "
                      f"output stops, consumption continues] " + "=" * 10)
            if chunk.thinking:
                n_thinking += 1
                thinking_chars += len(chunk.thinking)
                if not token._cancelled:
                    print(f"\033[2m[think #{n_thinking}] "
                          f"{chunk.thinking}\033[0m", end="", flush=True)
            if chunk.text:
                n_content += 1
                content_chars += len(chunk.text)
                if not token._cancelled:
                    print(chunk.text, end="", flush=True)
            if chunk.finish:
                finish_seen = True
                print("\n== [finish chunk received] " + "=" * 30)
    finally:
        await client.aclose()

    elapsed = time.monotonic() - t0
    payload = audit.last_payload

    print("\n== stream end " + "=" * 52)
    print(f"content chunks:   {n_content}  ({content_chars} chars)")
    print(f"thinking chunks:  {n_thinking}  ({thinking_chars} chars)")
    print(f"finish chunk:     {finish_seen}")
    if cancelled_at is not None:
        print(f"cancelled at:     {cancelled_at:.1f}s")
        print(f"drain window:     {elapsed - cancelled_at:.1f}s "
              f"(consumption after cancel until stream end)")
    print(f"elapsed:          {elapsed:.1f}s")
    print("== audit payload " + "=" * 49)
    for key in ("termination", "finish_reason", "stream_chunks",
                "usage", "cost_units", "elapsed_ms", "error",
                "cancelled", "interrupted"):
        if key in payload:
            print(f"{key:16s}: {payload[key]}")
    if not payload:
        print("<no audit payload captured>")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))