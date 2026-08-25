"""ID generation: unified format {prefix}_{uuid4hex[:12]}."""
from __future__ import annotations

import secrets
import uuid


def _short() -> str:
    return uuid.uuid4().hex[:12]


def new_stream_id() -> str:
    return f"s_{_short()}"


def new_placeholder_id() -> str:
    return f"ph_{_short()}"


def new_local_job_id() -> str:
    """Local mode enrich task ID (manifest task_ref prefix local:)."""
    return f"job_{_short()}"


def new_resume_token() -> str:
    """Resume token: prevents cross-session mixing, binds to this streaming session (v1 reserved, generation-only for delivery)."""
    return secrets.token_urlsafe(24)