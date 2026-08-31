"""Workflow tasks for the dependency-governance demo.

LeafTask is a plain governed node. FanInTask reads its parents' results
from the hot records — demonstrating that the dependency graph answers
STRUCTURE (who depends on whom) while the snapshot/hot record answers
STATE (what did they produce).
"""

from __future__ import annotations

import asyncio

from orditect.flow import BaseBackEndTask


class LeafTask(BaseBackEndTask):
    """A plain dependency parent: does work, optionally fails."""

    def __init__(self, storage, work: float = 0.1, fail: bool = False) -> None:
        super().__init__(storage)
        self._work = work
        self._fail = fail

    async def execute(self, task_id: str, **kwargs) -> dict:
        await asyncio.sleep(self._work)
        if self._fail:
            raise RuntimeError(f"simulated parent failure: {task_id}")
        return {"leaf": task_id, "value": f"output-of-{task_id}"}


class FanInTask(BaseBackEndTask):
    """The multi-parent child: consumes its parents' results."""

    def __init__(self, storage, needs: list[str]) -> None:
        super().__init__(storage)
        self._needs = needs

    async def execute(self, task_id: str, **kwargs) -> dict:
        inputs: dict[str, object] = {}
        for parent_id in self._needs:
            record = await self.storage.get_task(parent_id)
            inputs[parent_id] = record.get("result")
        return {"merged": inputs}


def make_task_factory(storage):
    """Build the task_factory (kept for symmetry with the other demos;

    RecoveryService is not used in this demo, but callers wiring resume/
    rerun over fan-in trees need every node registered here.
    """

    async def factory(task_id: str):
        if task_id in ("a", "b", "x", "y"):
            return LeafTask(storage)
        if task_id == "c1":
            return FanInTask(storage, needs=["a", "b"])
        raise KeyError(f"unknown task_id: {task_id}")

    return factory