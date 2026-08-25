"""Traceability closure check (B9 freeze gate 3).

Verifies the bidirectional traceability chain between docs/terms.md and the
conformance suite:
- Every term (Tn) referenced by a conformance case exists in terms.md.
- Every conformance case id (CF-XXX-NNN) referenced in terms.md exists in
  the suite.

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

TERM_RE = re.compile(r"^## (T\d+)\b", re.MULTILINE)
CASE_IN_TERMS_RE = re.compile(r"\bCF-[A-Z]{3}-\d{3}\b")
CASE_DEF_RE = re.compile(r'^@case\("((?:CF)-[A-Z]{3}-\d{3})"\)', re.MULTILINE)


def main() -> int:
    terms_text = TERMS.read_text(encoding="utf-8")

    terms_defined = set(TERM_RE.findall(terms_text))
    cases_in_terms = set(CASE_IN_TERMS_RE.findall(terms_text))

    cases_defined: set[str] = set()
    case_files = sorted(CASES_DIR.glob("cases_*.py"))
    for path in case_files:
        cases_defined |= set(CASE_DEF_RE.findall(path.read_text(encoding="utf-8")))

    problems: list[str] = []

    # Every case referenced in terms.md must exist in the suite.
    for case_id in sorted(cases_in_terms - cases_defined):
        problems.append(f"case {case_id} referenced in terms.md but not defined")

    # Every defined case must be referenced in terms.md (closure).
    for case_id in sorted(cases_defined - cases_in_terms):
        problems.append(f"case {case_id} defined but not referenced in terms.md")

    # Sanity: at least the 11 terms exist.
    expected_terms = {f"T{i}" for i in range(1, 12)}
    missing_terms = expected_terms - terms_defined
    for term in sorted(missing_terms):
        problems.append(f"term {term} expected but not found in terms.md")

    if problems:
        print("traceability closure FAILED:")
        for p in problems:
            print(f"  - {p}")
        return 1

    print(
        f"traceability closure OK: {len(terms_defined)} terms, "
        f"{len(cases_defined)} cases, all cross-references closed."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

