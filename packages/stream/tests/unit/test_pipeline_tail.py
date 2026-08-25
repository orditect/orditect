"""TailCleaner 单测。"""
import pytest

from orditect.stream.pipeline import TailCleaner, aiter_from_iterable


async def _run(chunks):
    out = []
    async for t in TailCleaner().process(aiter_from_iterable(chunks)):
        out.append(t)
    return out


class TestTailCleaner:
    async def test_trailing_whitespace_dropped(self):
        assert await _run(["正文", "\n", "  ", "\n"]) == ["正文"]

    async def test_inner_whitespace_kept(self):
        assert await _run(["a", "\n", "b"]) == ["a", "\n", "b"]

    async def test_all_whitespace(self):
        assert await _run(["\n", "  "]) == []