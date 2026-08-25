"""退避策略单元测试"""
import pytest

from orditect.flow import (
    ExponentialBackoff,
    LinearBackoff,
    ConstantBackoff,
)


class TestBackoffStrategies:
    """退避策略测试"""

    def test_exponential_backoff(self):
        """指数退避"""
        backoff = ExponentialBackoff(base=1.0, multiplier=2.0)
        assert backoff.calculate(0) == 1.0
        assert backoff.calculate(1) == 2.0
        assert backoff.calculate(2) == 4.0
        assert backoff.calculate(3) == 8.0

    def test_exponential_backoff_max_delay(self):
        """指数退避（最大延迟）"""
        backoff = ExponentialBackoff(base=1.0, multiplier=2.0, max_delay=5.0)
        assert backoff.calculate(0) == 1.0
        assert backoff.calculate(1) == 2.0
        assert backoff.calculate(2) == 4.0
        assert backoff.calculate(3) == 5.0  # 被 max_delay 限制

    def test_linear_backoff(self):
        """线性退避"""
        backoff = LinearBackoff(base=1.0, increment=1.0)
        assert backoff.calculate(0) == 1.0
        assert backoff.calculate(1) == 2.0
        assert backoff.calculate(2) == 3.0

    def test_constant_backoff(self):
        """固定退避"""
        backoff = ConstantBackoff(delay=5.0)
        assert backoff.calculate(0) == 5.0
        assert backoff.calculate(1) == 5.0
        assert backoff.calculate(2) == 5.0