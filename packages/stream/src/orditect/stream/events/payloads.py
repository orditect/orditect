"""Event payload models + construction factories.
- One payload model per event type (pydantic, with ext slot)
- make_* factories return data dict (inject ext; omit key if ext is empty to keep frame compact)
- runner/pipeline do not manually construct dicts, use these uniformly
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from orditect.stream.events.types import (
    DeltaKind,
    ErrorCode,
    PlaceholderState,
)


def _drop_none(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


# ---------------------------------------------------------------------------
# stream.start
# ---------------------------------------------------------------------------
class StreamStartPayload(BaseModel):
    stages: list[str]
    resume_token: str
    config_echo: dict[str, Any] = {}
    ext: dict[str, Any] = {}


def make_stream_start(
    stages: list[str],
    resume_token: str,
    config_echo: dict[str, Any] | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = StreamStartPayload(
        stages=stages,
        resume_token=resume_token,
        config_echo=config_echo or {},
        ext=ext or {},
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stream.delta
# ---------------------------------------------------------------------------
class DeltaPayload(BaseModel):
    kind: DeltaKind
    text: str | None = None
    references: list[dict[str, Any]] | None = None
    ext: dict[str, Any] = {}


def make_delta(
    kind: DeltaKind,
    text: str | None = None,
    references: list[dict[str, Any]] | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = DeltaPayload(kind=kind, text=text, references=references, ext=ext or {})
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# enrich.marker / enrich.placeholder / enrich.resolved
# ---------------------------------------------------------------------------
class EnrichMarkerPayload(BaseModel):
    placeholder_id: str
    context_text: str
    ext: dict[str, Any] = {}


def make_enrich_marker(
    placeholder_id: str,
    context_text: str,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = EnrichMarkerPayload(
        placeholder_id=placeholder_id, context_text=context_text, ext=ext or {}
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


class EnrichPlaceholderPayload(BaseModel):
    placeholder_id: str
    loading_url: str
    char_offset: int | None = None  # P0: 位置锚点
    ext: dict[str, Any] = {}


def make_enrich_placeholder(
    placeholder_id: str,
    loading_url: str,
    char_offset: int | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = EnrichPlaceholderPayload(
        placeholder_id=placeholder_id,
        loading_url=loading_url,
        char_offset=char_offset,
        ext=ext or {},
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


class EnrichResolvedPayload(BaseModel):
    placeholder_id: str
    url: str
    state: PlaceholderState
    ext: dict[str, Any] = {}


def make_enrich_resolved(
    placeholder_id: str,
    url: str,
    state: PlaceholderState = PlaceholderState.RESOLVED,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = EnrichResolvedPayload(
        placeholder_id=placeholder_id, url=url, state=state, ext=ext or {}
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stage.end
# ---------------------------------------------------------------------------
class StageResultPayload(BaseModel):
    content: str
    thinking: str | None = None


class StageEndPayload(BaseModel):
    name: str
    result: StageResultPayload
    usage: dict[str, Any] | None = None
    ext: dict[str, Any] = {}


def make_stage_end(
    name: str,
    content: str,
    thinking: str | None = None,
    usage: dict[str, Any] | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = StageEndPayload(
        name=name,
        result=StageResultPayload(content=content, thinking=thinking),
        usage=usage,
        ext=ext or {},
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stream.manifest
# ---------------------------------------------------------------------------
class ManifestPlaceholder(BaseModel):
    placeholder_id: str
    task_ref: str            # "tf:task-xxx" / "local:job-xxx"
    state: PlaceholderState
    stage: str | None = None       # P0: 归属 stage
    char_offset: int | None = None # P0: 在 stage 聚合 content 中的插入位置
    fallback_url: str | None = None
    url: str | None = None         # 已 resolved 时的真实地址


class ManifestPayload(BaseModel):
    stages: dict[str, StageResultPayload]
    placeholders: list[ManifestPlaceholder]
    usage: dict[str, Any] | None = None
    errors: list[dict[str, Any]] = []
    ext: dict[str, Any] = {}


def make_manifest(
    stages: dict[str, StageResultPayload],
    placeholders: list[ManifestPlaceholder],
    usage: dict[str, Any] | None = None,
    errors: list[dict[str, Any]] | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = ManifestPayload(
        stages=stages,
        placeholders=placeholders,
        usage=usage,
        errors=errors or [],
        ext=ext or {},
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stream.error
# ---------------------------------------------------------------------------
class StreamErrorPayload(BaseModel):
    code: ErrorCode
    message: str
    retryable: bool = False
    stage: str | None = None
    ext: dict[str, Any] = {}


def make_stream_error(
    code: ErrorCode,
    message: str,
    retryable: bool = False,
    stage: str | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    p = StreamErrorPayload(
        code=code, message=message, retryable=retryable, stage=stage, ext=ext or {}
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stream.cancelled (new)
# ---------------------------------------------------------------------------
class StreamCancelledPayload(BaseModel):
    """stream.cancelled event payload."""

    reason: str
    cancelled_at: float
    partial_content: str | None = None  # 中断时的部分内容（供业务层保存历史）
    ext: dict[str, Any] = {}


def make_stream_cancelled(
    reason: str,
    cancelled_at: float,
    partial_content: str | None = None,
    ext: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct stream.cancelled event payload."""
    p = StreamCancelledPayload(
        reason=reason,
        cancelled_at=cancelled_at,
        partial_content=partial_content,
        ext=ext or {},
    )
    return _drop_none(p.model_dump(exclude_defaults=True)) | (
        {"ext": p.ext} if p.ext else {}
    )


# ---------------------------------------------------------------------------
# stream.end (no business payload)
# ---------------------------------------------------------------------------
def make_stream_end(ext: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"ext": ext} if ext else {}