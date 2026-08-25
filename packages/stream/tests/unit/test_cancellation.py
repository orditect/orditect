"""CancellationToken 单测。"""
import asyncio

import pytest

from orditect.stream.core import CancellationToken
from orditect.stream.exceptions import StreamCancelledError


class TestCancellationToken:
    async def test_initial_state(self):
        token = CancellationToken()
        assert token.is_cancelled() is False
        assert token.reason is None
        assert token.cancelled_at is None

    async def test_cancel(self):
        token = CancellationToken()
        token.cancel("user_interrupt")
        assert token.is_cancelled() is True
        assert token.reason == "user_interrupt"
        assert token.cancelled_at is not None

    async def test_cancel_idempotent(self):
        token = CancellationToken()
        token.cancel("first")
        token.cancel("second")  # 第二次取消无效
        assert token.reason == "first"

    async def test_wait(self):
        token = CancellationToken()

        async def cancel_later():
            await asyncio.sleep(0.05)
            token.cancel("later")

        task = asyncio.create_task(cancel_later())
        await token.wait()
        await task
        assert token.is_cancelled() is True

    async def test_throw_if_cancelled(self):
        token = CancellationToken()
        token.throw_if_cancelled()  # 不抛异常

        token.cancel("test")
        with pytest.raises(StreamCancelledError):
            token.throw_if_cancelled()