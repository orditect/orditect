"""Seed fixtures for the consumer profile (language-neutral data shapes).

The dict shapes below are the Python expression of the future YAML test
vectors — the M4 data-rule toolkit shares this data lineage (see
docs/conformance.md). Adapter `seed(fixtures)` implementations consume the
per-domain lists of model payloads; seeding MUST be idempotent (T4) since
seed may be invoked repeatedly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

_PAST = datetime.now(UTC) - timedelta(seconds=1)


def consumer_fixtures() -> dict[str, list[dict]]:
    """Return fresh fixture payloads (new datetimes per call).

    Tree under test:
        cv-root
        ├── cv-a (generations e1=done, e2=running)
        │     └── cv-a1 (e1, done)
        └── cv-b (e1, failed)
        cv-expired (e1, running, expire_at in the past — invisible, T1)

    Edges: cv-a -> cv-root (primary), cv-b -> cv-root, cv-a1 -> cv-a.
    """
    return {
        "snapshots": [
            {"task_id": "cv-root", "step": "execute", "execution_id": "e1",
             "status": "done"},
            {"task_id": "cv-a", "step": "execute", "execution_id": "e1",
             "parent_task_id": "cv-root", "status": "done"},
            {"task_id": "cv-a", "step": "execute", "execution_id": "e2",
             "parent_task_id": "cv-root", "status": "running"},
            {"task_id": "cv-a1", "step": "execute", "execution_id": "e1",
             "parent_task_id": "cv-a", "status": "done"},
            {"task_id": "cv-b", "step": "execute", "execution_id": "e1",
             "parent_task_id": "cv-root", "status": "failed"},
            {"task_id": "cv-expired", "step": "execute", "execution_id": "e1",
             "parent_task_id": "cv-root", "status": "running",
             "expire_at": _PAST.isoformat()},
        ],
        "edges": [
            {"child_id": "cv-a", "parent_id": "cv-root", "is_primary": True},
            {"child_id": "cv-b", "parent_id": "cv-root", "is_primary": False},
            {"child_id": "cv-a1", "parent_id": "cv-a", "is_primary": False},
        ],
    }