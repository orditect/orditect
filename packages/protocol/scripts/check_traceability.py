"""Traceability closure check (three-way: terms <-> CF cases <-> DR rules).

Verifies the bidirectional traceability chain between docs/terms.md, the
conformance suite (CF-*), and the data-rule registry (DR-*):
- Every CF case referenced by terms.md exists in the suite, and vice versa.
- Every DR rule referenced by terms.md exists in the rule registry, and
  vice versa.
- All expected terms (T1..T12) exist in terms.md.

Exit code 0 = closure holds; 1 = a gap was found.

Run: python scripts/check_traceability.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
TERMS = ROOT / "docs" / "terms.md"
CASES_DIR = ROOT / "src" / "orditect" / "protocol" / "conformance"
RULES_RUNNER = ROOT / "src" / "orditect" / "protocol" / "rules" / "runner.py"

TERM_RE = re.compile(r"^## (T\d+)\b", re.MULTILINE)
CF_RE = re.compile(r"\bCF-[A-Z]{3}-\d{3}\b")
DR_RE = re.compile(r"\bDR-[A-Z]{3}-\d{3}\b")
CASE_DEF_RE = re.compile(r'^@case\("((?:CF)-[A-Z]{3}-\d{3})"\)', re.MULTILINE)
RULE_DEF_RE = re.compile(r'^\s*"(DR-[A-Z]{3}-\d{3})":', re.MULTILINE)


def main() -> int:
    terms_text = TERMS.read_text(encoding="utf-8")

    terms_defined = set(TERM_RE.findall(terms_text))
    cfs_in_terms = set(CF_RE.findall(terms_text))
    drs_in_terms = set(DR_RE.findall(terms_text))

    cfs_defined: set[str] = set()
    for path in sorted(CASES_DIR.glob("cases_*.py")):
        cfs_defined |= set(CASE_DEF_RE.findall(path.read_text(encoding="utf-8")))

    drs_defined: set[str] = set()
    if RULES_RUNNER.is_file():
        drs_defined = set(
            RULE_DEF_RE.findall(RULES_RUNNER.read_text(encoding="utf-8"))
        )

    problems: list[str] = []

    # CF two-way closure
    for case_id in sorted(cfs_in_terms - cfs_defined):
        problems.append(f"case {case_id} referenced in terms.md but not defined")
    for case_id in sorted(cfs_defined - cfs_in_terms):
        problems.append(f"case {case_id} defined but not referenced in terms.md")

    # DR two-way closure
    for rule_id in sorted(drs_in_terms - drs_defined):
        problems.append(f"rule {rule_id} referenced in terms.md but not registered")
    for rule_id in sorted(drs_defined - drs_in_terms):
        problems.append(f"rule {rule_id} registered but not referenced in terms.md")

    # Sanity: all expected terms exist.
    expected_terms = {f"T{i}" for i in range(1, 13)}
    for term in sorted(expected_terms - terms_defined):
        problems.append(f"term {term} expected but not found in terms.md")

    if problems:
        print("traceability closure FAILED:")
        for p in problems:
            print(f"  - {p}")
        return 1

    print(
        f"traceability closure OK: {len(terms_defined)} terms, "
        f"{len(cfs_defined)} cases, {len(drs_defined)} rules, "
        f"all cross-references closed."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())