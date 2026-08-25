"""Stream configuration: all tunable policies in one place.

- One global default StreamConfig
- Request-level override with merge(): None fields do not override global values
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Literal


class ThinkingMode(str, Enum):
    """How thinking deltas are handled."""

    INLINE = "inline"        # 作为 stream.delta kind=thinking 实时下发
    SEPARATE = "separate"    # 不下发实时流，仅聚合进 stage.end.result.thinking
    SUPPRESS = "suppress"    # 直接丢弃


class DisconnectPolicy(str, Enum):
    """Policy when client disconnects."""

    CANCEL = "cancel"      # 立即级联取消
    GRACE = "grace"        # 宽限期缓冲，重连倾倒，超时降级 cancel
    CONTINUE = "continue"  # 不取消，跑完落 store 供 refetch


class EnrichMode(str, Enum):
    """Backend for enrich task execution."""

    LOCAL = "local"        # 本地协程 + 注入的 enricher（mock 可用）
    TASKFLOW = "taskflow"  # 走 orditect-flow TaskOrchestrator
    AUTO = "auto"          # 自动检测 taskflow，缺失则降级 local


class BackpressurePolicy(str, Enum):
    """Policy when mux queue is full. drop_oldest not provided (dropping text deltas breaks integrity)."""

    BLOCK = "block"  # 反压上游源（默认）
    FAIL = "fail"    # 抛 BackpressureError，转 stream.error


@dataclass(frozen=True)
class StreamConfig:
    """Stream configuration (immutable).

    Any wait has an upper bound (settle/grace/heartbeat), never blocks indefinitely.
    """

    thinking_mode: ThinkingMode = ThinkingMode.INLINE
    on_disconnect: DisconnectPolicy = DisconnectPolicy.GRACE
    grace_period: float = 30.0
    enrich_settle_timeout: float = 5.0
    heartbeat_interval: float = 15.0
    queue_maxsize: int = 1000
    backpressure: BackpressurePolicy = BackpressurePolicy.BLOCK
    enrich_mode: EnrichMode = EnrichMode.AUTO
    result_ttl: int = 86400

    # marker detection parameters
    marker: str = "![img]"
    marker_flush_threshold: int = 50     # 缓冲达此长度且遇触发点则冲刷
    marker_flush_timeout: float = 0.1    # 超过此秒数未冲刷则强制冲刷

    # enrich context extraction strategy: how to truncate text before marker
    # "paragraph" nearest paragraph (default)
    # "heading"   from nearest heading
    # "full"      entire text
    enrich_context_strategy: Literal["paragraph", "heading", "full"] = "paragraph"

    # new: resource governance
    governor_timeout: float = 30.0  # governor acquire 超时（秒）
    # v0.3.0(1b): fallback limit (seconds) for continuing to consume LLM after cancel.
    # Aligned with "any wait has a limit" discipline — prevents semaphore permanent occupation when LLM hangs.
    post_cancel_drain_timeout: float = 30.0

    def merge(self, **overrides) -> "StreamConfig":
        """Request-level override: only non-None fields take effect, returns a new instance."""
        valid = {f.name for f in dataclasses.fields(self)}
        updates = {k: v for k, v in overrides.items() if k in valid and v is not None}
        return dataclasses.replace(self, **updates)


DEFAULT_CONFIG = StreamConfig()