"""Newline normalization (safe across chunk boundaries).

- CRLF/CR unified to LF
- Multiple \n within a chunk merged into single \n
- No double \n across chunk boundaries (pending trailing \n merges with next chunk's leading \n)
- Do not output a pending orphan \n at the end
"""
from __future__ import annotations

import re
from collections.abc import AsyncIterable, AsyncIterator

_CRLF_RE = re.compile(r"\r\n|\r")
_MULTI_NL_RE = re.compile(r"\n+")


class NewlineNormalizer:
    """Newline normalization middleware."""

    async def process(self, chunks: AsyncIterable[str]) -> AsyncIterator[str]:
        pending_nl = False
        async for chunk in chunks:
            t = _CRLF_RE.sub("\n", chunk)
            t = _MULTI_NL_RE.sub("\n", t)
            if pending_nl:
                pending_nl = False
                t = "\n" + t.lstrip("\n")
            if t.endswith("\n"):
                pending_nl = True
                t = t[:-1]
            if t:
                yield t
        # pending lone \n not output
