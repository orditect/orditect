"""Shared filesystem helpers for the local-file adapter.

Discipline:
- Writes are single-process and write-atomic: serialize -> tmp file ->
  os.replace. A reader never observes a partially written file.
- Locking is per-part asyncio locks (concurrency_domain = "process").
- Streams (ndjson) are append-only; readers fold them into query views.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def utc_now_iso() -> str:
    """Envelope timestamp: timezone-aware UTC ISO string (T7)."""
    return datetime.now(UTC).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str) -> None:
    """Write-atomic file replace (tmp file in the same dir + os.replace)."""
    ensure_dir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_bytes(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def append_envelope(path: Path, op: str, data: dict) -> None:
    """Append one op-envelope row (single-line ndjson) to a stream file."""
    ensure_dir(path.parent)
    row = {"v": 1, "op": op, "ts": utc_now_iso(), "data": data}
    line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def read_json(path: Path) -> dict | None:
    """Read one JSON file; None when missing or unparsable."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def iter_envelopes(path: Path) -> list[dict]:
    """Fold an ndjson stream file into envelope dicts (skips bad rows)."""
    if not path.is_file():
        return []
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict) and isinstance(row.get("data"), dict):
                rows.append(row)
    return rows


def parse_dt(value) -> datetime | None:
    """Parse an ISO datetime string (T7: explicit offset expected)."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None