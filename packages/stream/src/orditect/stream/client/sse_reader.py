"""SSEClient: standard SSE frame parsing (async + sync dual versions).

Parsing rules:
- event:/id:/data: fields accumulate, empty line triggers a frame
- data multi-line reassembled with \n
- Comment lines (starting with :) ignored
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field


@dataclass
class RawSSEFrame:
    """Raw SSE frame (not deserialized)."""

    event: str = "message"
    data_lines: list[str] = field(default_factory=list)
    id: str | None = None

    @property
    def data(self) -> str:
        return "\n".join(self.data_lines)

    def reset(self) -> "RawSSEFrame":
        return RawSSEFrame()


class _FrameParser:
    """Line-level parser (shared by async/sync)."""

    def __init__(self):
        self._cur = RawSSEFrame()

    def feed_line(self, line: str) -> RawSSEFrame | None:
        """Feed one line, return frame if complete, otherwise None."""
        line = line.rstrip("\r\n")
        if line == "":
            # empty line: frame boundary
            if self._cur.data_lines or self._cur.id or self._cur.event != "message":
                frame = self._cur
                self._cur = RawSSEFrame()
                return frame
            return None
        if line.startswith(":"):
            return None  # 注释/心跳
        if line.startswith("event:"):
            self._cur.event = line[6:].strip()
        elif line.startswith("id:"):
            self._cur.id = line[3:].strip()
        elif line.startswith("data:"):
            # data: may have one leading space
            value = line[5:]
            if value.startswith(" "):
                value = value[1:]
            self._cur.data_lines.append(value)
        return None


class SSEClient:
    """SSE client frame parser."""

    @staticmethod
    async def aparse(byte_stream: AsyncIterator[bytes]) -> AsyncIterator[RawSSEFrame]:
        """Async version: byte stream → frame stream."""
        parser = _FrameParser()
        buffer = ""
        async for chunk in byte_stream:
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                frame = parser.feed_line(line)
                if frame is not None:
                    yield frame
        # cleanup: process remaining buffer as one line
        if buffer:
            frame = parser.feed_line(buffer)
            if frame is not None:
                yield frame

    @staticmethod
    def parse(byte_stream: Iterator[bytes]) -> Iterator[RawSSEFrame]:
        """Sync version: byte stream → frame stream."""
        parser = _FrameParser()
        buffer = ""
        for chunk in byte_stream:
            buffer += chunk.decode("utf-8", errors="replace")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                frame = parser.feed_line(line)
                if frame is not None:
                    yield frame
        if buffer:
            frame = parser.feed_line(buffer)
            if frame is not None:
                yield frame