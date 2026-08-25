"""Tail cleanup: trailing newlines/whitespace at the end of text are not output."""
from __future__ import annotations

import re
from collections.abc import AsyncIterable, AsyncIterator

_WS_ONLY_RE = re.compile(r"^\s*$")


class TailCleaner:
    """Buffer whitespace-only chunks, flush on real content; discard trailing whitespace at end."""

    async def process(self, chunks: AsyncIterable[str]) -> AsyncIterator[str]:
        buffer: list[str] = []
        async for chunk in chunks:
            if _WS_ONLY_RE.fullmatch(chunk):
                buffer.append(chunk)
                continue
            for c in buffer:
                yield c
            buffer.clear()
            yield chunk
        # end: residual whitespace not output
