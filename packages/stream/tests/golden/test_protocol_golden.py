"""Golden 协议快照：冻结事件序列结构与字段集合。

纪律：
- 协议变更（新事件/字段改名/载荷结构变化）必须先更新本快照并评审
- 不比对 ts/seq/id（运行时变化），只比对事件类型序列 + 字段集合 + 关键值
"""
import pytest

from orditect.stream.events import (
    DeltaKind,
    ErrorCode,
    EventType,
    PlaceholderState,
    make_delta,
    make_enrich_marker,
    make_enrich_placeholder,
    make_enrich_resolved,
    make_manifest,
    make_stage_end,

    make_stream_error,
    make_stream_start,
    ManifestPlaceholder,
    StageResultPayload,
    make_stream_cancelled,
)

pytestmark = pytest.mark.golden


class TestEventTypeEnum:
    """事件类型枚举冻结。"""

    def test_event_types(self):
        assert {e.value for e in EventType} == {
            "stream.start",
            "stream.delta",
            "enrich.marker",
            "enrich.placeholder",
            "enrich.resolved",
            "stage.end",
            "stream.manifest",
            "stream.end",
            "stream.error",
            "stream.cancelled",  # 新增
        }

    def test_delta_kinds(self):
        assert {e.value for e in DeltaKind} == {"content", "thinking", "references"}

    def test_placeholder_states(self):
        assert {e.value for e in PlaceholderState} == {"pending", "resolved", "failed"}

    def test_error_codes(self):
        assert {e.value for e in ErrorCode} == {
            "SOURCE_ERROR", "ENRICH_ERROR", "TIMEOUT",
            "CANCELLED", "BACKPRESSURE", "UPSTREAM_INTERRUPTED", "INTERNAL",
        }


class TestPayloadSchema:
    """载荷 schema 冻结（字段集合）。"""

    def test_stream_start(self):
        p = make_stream_start(stages=["a", "b"], resume_token="tok")
        assert set(p.keys()) == {"stages", "resume_token"}
        assert p["stages"] == ["a", "b"]

    def test_delta(self):
        p = make_delta(DeltaKind.CONTENT, text="hi")
        assert p["kind"] == "content"
        assert p["text"] == "hi"
        assert "references" not in p  # None 省略

    def test_enrich_marker(self):
        p = make_enrich_marker(placeholder_id="ph_1", context_text="ctx")
        assert set(p.keys()) == {"placeholder_id", "context_text"}

    def test_enrich_placeholder(self):
        p = make_enrich_placeholder(placeholder_id="ph_1", loading_url="l.jpg", char_offset=42)
        assert set(p.keys()) == {"placeholder_id", "loading_url", "char_offset"}
        assert p["char_offset"] == 42

    def test_enrich_resolved(self):
        p = make_enrich_resolved(placeholder_id="ph_1", url="r.jpg")
        assert set(p.keys()) == {"placeholder_id", "url", "state"}
        assert p["state"] == "resolved"

    def test_stage_end(self):
        p = make_stage_end(name="main", content="c", thinking="t")
        assert p["name"] == "main"
        assert p["result"] == {"content": "c", "thinking": "t"}

    def test_manifest(self):
        p = make_manifest(
            stages={"main": StageResultPayload(content="c")},
            placeholders=[ManifestPlaceholder(
                placeholder_id="ph_1", task_ref="tf:t1",
                state=PlaceholderState.PENDING,
                stage="main", char_offset=10,
            )],
        )
        assert "stages" in p
        assert "placeholders" in p
        ph = p["placeholders"][0]
        assert ph["task_ref"] == "tf:t1"
        assert ph["state"] == "pending"
        assert ph["stage"] == "main"
        assert ph["char_offset"] == 10
        assert "url" not in ph  # None 省略

    def test_stream_error(self):
        p = make_stream_error(ErrorCode.INTERNAL, "boom", retryable=True)
        assert p["code"] == "INTERNAL"
        assert p["retryable"] is True

    def test_ext_passthrough(self):
        p = make_delta(DeltaKind.CONTENT, text="hi", ext={"category": 3})
        assert p["ext"] == {"category": 3}

    def test_stream_cancelled(self):
        """stream.cancelled 事件载荷冻结。"""
        p = make_stream_cancelled(
            reason="user_interrupt",
            cancelled_at=1234567890.0,
            partial_content="中断时的内容",
        )
        assert p["reason"] == "user_interrupt"
        assert p["cancelled_at"] == 1234567890.0
        assert p["partial_content"] == "中断时的内容"