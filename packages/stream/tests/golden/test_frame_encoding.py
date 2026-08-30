"""Golden frame-encoding freeze (v0.1.7 postmortem).

The SSE frame's byte shape is protocol surface: consumers recognize
business frames by "id:" and heartbeat comment frames by ":ping". A
malformed frame (e.g. the ": :ping" double-prefix bug found in the v0.1.7
heartbeat postmortem) is invisible to every consumer and to prefix-based
assertions, while appearing "present" at the object level. These pins
freeze the exact wire bytes so any such drift fails loudly.

Discipline (aligned with test_protocol_golden.py):
- Frame format changes must update this snapshot in the same commit and
  pass review.
- ts/seq/id are pinned with fixed inputs here (byte-exact output is the
  point); event-type sequencing stays in test_protocol_golden.py.
"""

import pytest

from orditect.stream.events import (
    DeltaKind,
    EventEnvelope,
    EventType,
    make_delta,
)
from orditect.stream.sse import (
    SSEFrame,
    encode_envelope,
    encode_heartbeat,
)

pytestmark = pytest.mark.golden


class TestHeartbeatFrame:
    def test_heartbeat_bytes_exact(self):
        """Normalized comment frame: ":ping" with no double prefix."""
        assert encode_heartbeat() == b":ping\n\n"


class TestEnvelopeFrame:
    def test_business_frame_bytes_exact(self):
        env = EventEnvelope(
            stream_id="s1",
            seq=1,
            ts=1.0,
            data=make_delta(DeltaKind.CONTENT, text="hello"),
        )
        frame = encode_envelope(env, EventType.STREAM_DELTA)
        assert frame == (
            b"id: s1:1\n"
            b"event: stream.delta\n"
            b'data: {"v":1,"stream_id":"s1","seq":1,"ts":1.0,'
            b'"data":{"kind":"content","text":"hello"}}\n'
            b"\n"
        )

    def test_frame_with_stage_includes_stage_field(self):
        env = EventEnvelope(
            stream_id="s1",
            stage="main",
            seq=7,
            ts=2.0,
            data={},
        )
        frame = encode_envelope(env, EventType.STREAM_END)
        assert frame.startswith(b"id: s1:7\nevent: stream.end\n")
        assert b'"stage":"main"' in frame
        assert frame.endswith(b"\n\n")


class TestDataLineSplitting:
    def test_multiline_data_splits_into_data_lines(self):
        """Literal newlines inside a hand-built data string must be split
        into separate data: lines (never a bare newline that would break
        the frame for consumers)."""
        frame = SSEFrame(
            event="x", data="line1\nline2\r\nline3\rline4", id="s:1"
        ).encode()
        assert frame.decode().split("\n") == [
            "id: s:1",
            "event: x",
            "data: line1",
            "data: line2",
            "data: line3",
            "data: line4",
            "",
            "",
        ]

    def test_comment_frame_verbatim_text(self):
        """Comment text is emitted verbatim after the colon (no added
        space, no added prefix)."""
        frame = SSEFrame(event="", data="", comment="keep-alive").encode()
        assert frame == b":keep-alive\n\n"