"""Lua time-source gate (clock discipline, T7 / Lua policy criterion 4).

Server-side clock rule for orditect-core Lua scripts: CLOCK READINGS
(instants: now / expire_at / slot timestamps) must come from
redis.call('TIME'), never from script arguments. Durations (TTL seconds,
lease lengths, refill frequencies) are caller-declared and legitimately
arrive via ARGV.

  L1  forbidden pattern: a numeric ARGV value flowing into an INSTANT-typed
      variable (now_ms / expire_at / slot_ms / deadline / server_now)
  L2  every script that performs time arithmetic must contain
      redis.call('TIME') (or a registered exemption below)

Exemptions carry a written reason; adding one requires two-person review.
This gate closes the transition window opened when the v0.1.1 diff-based
freeze gate was retired (see CHANGELOG, M0).

Run: python scripts/gates/check_lua_time_source.py
Exit 0 = clean; 1 = violation.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common import rel_posix, repo_root

_LUA_DIR = Path("packages/core/src/orditect/core/lua")

_TIMEISH = r"(?:now|time|expire|expiry|ttl|slot|deadline)"
#: tonumber(ARGV[n]) within 80 chars of a time-ish word (either direction)
#: Instant-valued words (clock readings): client-supplied values for these
#: pollute shared state — forbidden from ARGV.
_INSTANT_WORDS = r"(?:now_ms|now|expire_at|slot_ms|server_now|deadline|expire_at_ms)"

#: Duration-valued words (lease lengths, TTL seconds, frequencies): the
#: caller declares these; passing them via ARGV is the designed contract.
#: (Not used in patterns — documented here so reviewers can extend the list.)
_DURATION_WORDS = ("ttl", "lease", "expiry", "frequency", "interval",
                   "task_ttl", "lease_time", "refill_frequency")

_FORBIDDEN_PATTERNS = [
    re.compile(rf"tonumber\(ARGV\[\d+\]\).{{0,80}}{_INSTANT_WORDS}", re.IGNORECASE),
    re.compile(rf"{_INSTANT_WORDS}.{{0,80}}tonumber\(ARGV\[\d+\]\)", re.IGNORECASE),
]

_SERVER_CLOCK = "redis.call('TIME')"

#: scripts allowed to omit redis.call('TIME') — {relpath: reason}
_EXEMPTIONS: dict[str, str] = {
    "json_merge.lua": "pure RMW merge; no time arithmetic "
                      "(expiry is a caller-supplied TTL duration, not a clock read)",
    "sem_release.lua": "idempotent ZREM only; no time arithmetic",
    "quota_release.lua": "reads and restores the existing TTL (TTL/EXPIRE "
                         "passthrough); never computes an expiry instant",
}


def _uses_time_arithmetic(text: str) -> bool:
    """Heuristic: the script computes scores/lease windows/expiry instants."""
    return bool(re.search(r"(ZADD|ZREMRANGEBYSCORE|EXPIRE|expire_at|score)", text))


def main() -> int:
    root = repo_root()
    lua_dir = root / _LUA_DIR
    if not lua_dir.is_dir():
        print(f"error: lua directory not found: {lua_dir}", file=sys.stderr)
        return 1

    findings: list[str] = []
    scripts = sorted(lua_dir.glob("*.lua"))
    for path in scripts:
        rel = path.name
        text = path.read_text(encoding="utf-8")

        for pattern in _FORBIDDEN_PATTERNS:
            match = pattern.search(text)
            if match:
                findings.append(
                    f"{rel}: [L1] ARGV flows into a time value: "
                    f"{match.group(0)[:60]!r}"
                )

        if _uses_time_arithmetic(text) and _SERVER_CLOCK not in text:
            reason = _EXEMPTIONS.get(rel)
            if reason is None:
                findings.append(
                    f"{rel}: [L2] time arithmetic without redis.call('TIME') "
                    f"(or register an exemption with a written reason)"
                )

    if findings:
        print("lua-time-source gate FAILED:")
        for finding in findings:
            print(f"  - {finding}")
        return 1

    print(f"lua-time-source gate OK: {len(scripts)} scripts clean "
          f"({len(_EXEMPTIONS)} registered exemptions)")
    return 0


if __name__ == "__main__":
    sys.exit(main())