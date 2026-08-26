"""v0.1.1 freeze gate: the dependency-governance release must NOT touch
core Lua scripts or the protocol package. Exit 0 = frozen; 1 = violation.

Run: python scripts/check_v011_frozen.py [base_ref]
(base_ref defaults to v0.1.0; falls back to HEAD~10 when the tag is absent.)
"""

from __future__ import annotations

import subprocess
import sys

FORBIDDEN_PREFIXES = (
    "packages/core/src/orditect/core/lua/",
    "packages/protocol/",
)

# Build artifacts are auto-generated on reinstall; never treat them as
# contract changes.
IGNORED_MARKERS = (".egg-info/",)

def main() -> int:
    base = sys.argv[1] if len(sys.argv) > 1 else "v0.1.0"
    try:
        out = subprocess.run(
            ["git", "diff", "--name-only", f"{base}..HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout
    except subprocess.CalledProcessError:
        out = subprocess.run(
            ["git", "diff", "--name-only", "HEAD~10..HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout
        print(f"warn: base ref {base!r} not found; compared against HEAD~10")

    changed = [line.strip() for line in out.splitlines() if line.strip()]
    violations = [
        p for p in changed
        if p.startswith(FORBIDDEN_PREFIXES)
        and not any(m in p for m in IGNORED_MARKERS)
    ]
    if violations:
        print("v0.1.1 freeze gate FAILED — forbidden paths touched:")
        for p in violations:
            print(f"  - {p}")
        return 1
    print(f"v0.1.1 freeze gate OK: {len(changed)} files changed, none forbidden.")
    return 0


if __name__ == "__main__":
    sys.exit(main())