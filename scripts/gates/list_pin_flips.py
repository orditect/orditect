#!/usr/bin/env python3
"""Pinning-flip ledger generator (tool, never gates CI — exit code always 0).

Scans the test suites and the conformance kit for FLIP marker comments and
prints a markdown table grouped by version, ready to paste into the
CHANGELOG's "pinning flips" section.

Marker discipline (see CONTRIBUTING.md):
    # FLIP(v0.1.2): <reason for the semantic flip> — <source WI / doc section>

Run: python scripts/gates/list_pin_flips.py
"""

from __future__ import annotations

import re
from pathlib import Path

from common import PACKAGES, repo_root

FLIP_RE = re.compile(r"#\s*FLIP\((v[\d.]+)\):\s*(.+?)\s*$")


def _scan_file(root: Path, path: Path) -> list[tuple[str, str, int, str]]:
    """Yield (version, relpath, lineno, reason) for each FLIP marker."""
    out: list[tuple[str, str, int, str]] = []
    rel = path.relative_to(root).as_posix()
    for lineno, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        match = FLIP_RE.search(line)
        if match:
            out.append((match.group(1), rel, lineno, match.group(2)))
    return out


def _candidate_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for meta in PACKAGES.values():
        pkg_root = root / str(meta["path"])
        for base in (pkg_root / "tests",):
            if base.is_dir():
                files.extend(sorted(base.rglob("*.py")))
        conformance = (
            pkg_root / "src" / "orditect" / "protocol" / "conformance"
        )
        if conformance.is_dir():
            files.extend(sorted(conformance.rglob("*.py")))
    return files


def main() -> int:
    root = repo_root()
    entries: list[tuple[str, str, int, str]] = []
    for path in _candidate_files(root):
        entries.extend(_scan_file(root, path))

    if not entries:
        print("no FLIP markers found.")
        return 0

    by_version: dict[str, list[tuple[str, int, str]]] = {}
    for version, rel, lineno, reason in entries:
        by_version.setdefault(version, []).append((rel, lineno, reason))

    print("# Pinning-flip ledger\n")
    for version in sorted(by_version):
        print(f"## {version}\n")
        print("| file | line | reason |")
        print("|---|---|---|")
        for rel, lineno, reason in sorted(by_version[version]):
            print(f"| {rel} | {lineno} | {reason} |")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())