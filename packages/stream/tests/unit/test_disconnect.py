"""GraceBuffer / DisconnectMonitor 单测。"""
import asyncio

import pytest

from orditect.stream.config import DEFAULT_CONFIG, DisconnectPolicy
from orditect.stream.disconnect import DisconnectMonitor, GraceBuffer
from orditect.stream.events import EventEnvelope, EventType


def _env(sid="s1", seq=1):
    return EventEnvelope(stream_id=sid, seq=seq, data={})


class TestGraceBuffer:
    async def test_put_and_drain(self):
        buf = GraceBuffer()
        await buf.put(_env(seq=1), EventType.STREAM_DELTA)
        await buf.put(_env(seq=2), EventType.STREAM_DELTA)
        items, gap = await buf.drain()
        assert len(items) == 2
        assert gap is False
        assert await buf.size() == 0

    async def test_gap_on_overflow(self):
        buf = GraceBuffer(maxsize=2)
        for i in range(4):
            await buf.put(_env(seq=i), EventType.STREAM_DELTA)
        items, gap = await buf.drain()
        assert len(items) == 2      # 只留最新 2 条
        assert gap is True          # 标记有空洞


class TestDisconnectMonitor:
    async def test_cancel_policy_immediate(self):
        called = []

        async def on_cancel():
            called.append(True)

        cfg = DEFAULT_CONFIG.merge(on_disconnect=DisconnectPolicy.CANCEL)
        mon = DisconnectMonitor(cfg, on_cancel)
        await mon.notify_disconnect()
        assert called == [True]
        assert mon.is_cancelled is True

    async def test_grace_policy_reconnect_in_time(self):
        called = []

        async def on_cancel():
            called.append(True)

        cfg = DEFAULT_CONFIG.merge(
            on_disconnect=DisconnectPolicy.GRACE, grace_period=0.2
        )
        mon = DisconnectMonitor(cfg, on_cancel)
        await mon.notify_disconnect()
        assert mon.should_buffer is True

        # reconnect within period
        await asyncio.sleep(0.05)
        items, gap = await mon.notify_reconnect()
        assert mon.is_cancelled is False
        assert called == []
        assert mon.should_buffer is False
        await mon.close()

    async def test_grace_policy_timeout_cancels(self):
        called = []

        async def on_cancel():
            called.append(True)

        cfg = DEFAULT_CONFIG.merge(
            on_disconnect=DisconnectPolicy.GRACE, grace_period=0.05
        )
        mon = DisconnectMonitor(cfg, on_cancel)
        await mon.notify_disconnect()
        await asyncio.sleep(0.15)  # 超时不重连
        assert called == [True]
        assert mon.is_cancelled is True

    async def test_continue_policy_no_intervention(self):
        called = []

        async def on_cancel():
            called.append(True)

        cfg = DEFAULT_CONFIG.merge(on_disconnect=DisconnectPolicy.CONTINUE)
        mon = DisconnectMonitor(cfg, on_cancel)
        await mon.notify_disconnect()
        await asyncio.sleep(0.05)
        assert called == []
        assert mon.is_cancelled is False
        await mon.close()