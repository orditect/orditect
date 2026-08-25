"""Backoff strategy: calculate retry interval time."""
from abc import ABC, abstractmethod


class BackoffStrategy(ABC):
    """Base class for backoff strategies.

        Defines how to calculate retry interval time.
        """
    @abstractmethod
    def calculate(self, attempt: int) -> float:
        """Calculate backoff time in seconds.

                Args:
                    attempt: Current retry attempt number (starting from 0)

                Returns:
                    Backoff time in seconds
                """
        pass


class ExponentialBackoff(BackoffStrategy):
    """Exponential backoff strategy.

        Backoff time = base * (multiplier ^ attempt)

        Example:
            base=1.0, multiplier=2.0
            - Attempt 0: 1.0 * 2^0 = 1.0 sec
            - Attempt 1: 1.0 * 2^1 = 2.0 sec
            - Attempt 2: 1.0 * 2^2 = 4.0 sec
            - Attempt 3: 1.0 * 2^3 = 8.0 sec
        """

    def __init__(
            self,
            base: float = 1.0,
            multiplier: float = 2.0,
            max_delay: float = 60.0,
    ):
        """
                Args:
                    base: Base delay in seconds
                    multiplier: Multiplier factor per retry
                    max_delay: Maximum delay in seconds
                """
        self.base = base
        self.multiplier = multiplier
        self.max_delay = max_delay

    def calculate(self, attempt: int) -> float:
        """Calculate backoff time in seconds."""
        delay = self.base * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


class LinearBackoff(BackoffStrategy):
    """Linear backoff strategy.

        Backoff time = base + (increment * attempt)

        Example:
            base=1.0, increment=1.0
            - Attempt 0: 1.0 + 1.0*0 = 1.0 sec
            - Attempt 1: 1.0 + 1.0*1 = 2.0 sec
            - Attempt 2: 1.0 + 1.0*2 = 3.0 sec
            - Attempt 3: 1.0 + 1.0*3 = 4.0 sec
        """

    def __init__(
            self,
            base: float = 1.0,
            increment: float = 1.0,
            max_delay: float = 60.0,
    ):
        """
                Args:
                    base: Base delay in seconds
                    increment: Additional seconds per retry
                    max_delay: Maximum delay in seconds
                """
        self.base = base
        self.increment = increment
        self.max_delay = max_delay

    def calculate(self, attempt: int) -> float:
        """Calculate backoff time in seconds."""
        delay = self.base + (self.increment * attempt)
        return min(delay, self.max_delay)


class ConstantBackoff(BackoffStrategy):
    """Constant backoff strategy.

        Backoff time = delay (fixed value)

        Example:
            delay=5.0
            - Attempt 0: 5.0 sec
            - Attempt 1: 5.0 sec
            - Attempt 2: 5.0 sec
        """

    def __init__(self, delay: float = 1.0):
        """
                Args:
                    delay: Fixed delay in seconds
                """
        self.delay = delay

    def calculate(self, attempt: int) -> float:
        """Calculate backoff time in seconds."""
        return self.delay