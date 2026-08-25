"""Recovery plane (F4): resume / rerun primitives.

Built on top of:
- core reopen_task (hot-path new-generation opening),
- protocol snapshot domain (warm-path tree/version queries),
- F3 result reuse short-circuit.

Boundary discipline: the caller injects task_factory (how to reconstruct a
task instance from a task_id). The framework embeds no business semantics
(no task registry, no task-type knowledge) — mechanism to the framework,
semantics to the business.
"""
"""Recovery plane (F4): resume / rerun primitives."""
from orditect.flow.recovery.service import RecoveryService, ReuseDecision, TaskFactory

__all__ = ["RecoveryService", "ReuseDecision", "TaskFactory"]