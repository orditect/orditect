"""Distributed rate limiting: semaphore (concurrency control) + token bucket (rate control) + declarative decorator."""
from orditect.core.limiter.hooks import LimiterHooks
from orditect.core.limiter.semaphore import AsyncLeaseSemaphore, LeaseToken, SemaphoreHold
from orditect.core.limiter.bucket import AsyncTokenBucket
from orditect.core.limiter.registry import LimiterRegistry, get_registry
from orditect.core.limiter.decorators import limited

__all__ = [
    "LimiterHooks",
    "AsyncLeaseSemaphore",
    "LeaseToken",
    "SemaphoreHold",
    "AsyncTokenBucket",
    "LimiterRegistry",
    "get_registry",
    "limited",
]