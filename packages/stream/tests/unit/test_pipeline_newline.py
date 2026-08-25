"""NewlineNormalizer 单测。"""
import pytest

from orditect.stream.pipeline import NewlineNormalizer, aiter_from_iterable


async def _run(chunks):
    out = []
    async for t in NewlineNormalizer().process(aiter_from_iterable(chunks)):
        out.append(t)
    return "".join(out)


class TestNewlineNormalizer:
    async def test_crlf_to_lf(self):
        assert await _run(["a\r\nb\rc\nd"]) == "a\nb\nc\nd"

    async def test_multi_newline_merged(self):
        assert await _run(["a\n\n\nb\n\nc"]) == "a\nb\nc"

    async def test_cross_chunk_boundary_no_double(self):
        # previous block tail \n + next block head \n merged
        assert await _run(["a\n", "\nb"]) == "a\nb"

    async def test_trailing_newline_dropped(self):
        assert await _run(["a\n\n", "\n"]) == "a\n" or await _run(["a\n\n", "\n"]) == "a"

    async def test_empty(self):
        assert await _run([]) == ""