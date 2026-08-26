#!/usr/bin/env python3
"""Schema artifact generator for orditect-protocol (single source of truth).

Exports pydantic models to JSON Schema and applies the wire-format
post-processing rules, so the checked-in artifacts are always reproducible
by re-running this script. The drift gate (tests/golden/test_schema_drift.py)
re-runs the same generator and diffs — never hand-edit the artifacts.

Post-processing (each rule mirrors docs/wire-format.md; change the doc first):
  1. `anyOf: [T, null]` collapses to `T`, the contradictory `"default": null`
     is dropped, and the field description gains "Omitted when None."
     (wire-format: None-valued fields are omitted, never serialized as null)
  2. `format: date-time` fields get an explicit-offset pattern (T7: a
     timezone offset — "Z" or "+HH:MM" — is mandatory).
  3. `$id` / `$schema` / `title` / `x-stability` are injected.
  4. Field descriptions come from the DESCRIPTIONS map below — the ONLY
     place descriptions live.

Output is byte-deterministic: json.dumps(sort_keys=True, indent=2) + "\n".

Usage:
  python packages/protocol/scripts/generate_schemas.py           # write artifacts
  python packages/protocol/scripts/generate_schemas.py --check   # diff only, exit 1 on drift
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from pydantic import BaseModel
from orditect.protocol.capabilities import CapabilitySet

_PKG_ROOT = Path(__file__).resolve().parents[1]
_SRC = _PKG_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from orditect.protocol.models import (  # noqa: E402
    AuditEvent,
    DependencyEdge,
    DependencyGraph,
    Page,
    Sort,
    SortDirection,
    TaskPointer,
    TaskSnapshot,
    TimeRange,
)

SCHEMA_DIR = _PKG_ROOT / "schemas"
_JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"
_ID_BASE = "https://orditect.dev/schemas"
_STABILITY = "ratifying"
#: T7 data-level enforcement: explicit offset, "Z" or "+HH:MM"/"-HH:MM".
_EXPLICIT_OFFSET_PATTERN = r"(Z|[+-]\d{2}:\d{2})$"

#: Field descriptions — the single source of truth (T-term references live
#: here, not in the artifacts by hand-editing). {Model: {field: description}}
DESCRIPTIONS: dict[str, dict[str, str]] = {
    "TaskSnapshot": {
        "task_id": "Task identifier (deterministic-ID conventions may apply).",
        "step": "Step name within the task (opaque).",
        "execution_id": "Execution generation identity (T11). A new execution "
                        "of the same task (resume / rerun) uses a new "
                        "execution_id; multiple generations coexist.",
        "parent_task_id": "Parent task in the recursive lineage (None = root).",
        "status": "Opaque status string (T6); the protocol embeds no vocabulary.",
        "input_pointer": "Pointer-ized input content (T5); never inline payloads.",
        "output_pointer": "Pointer-ized output content (T5).",
        "error": "Error message if the execution failed (opaque text).",
        "cost": "Free-form cost metrics (business-defined).",
        "model": "Optional model identifier (business-defined).",
        "created_at": "Record creation time, timezone-aware UTC (T7).",
        "updated_at": "Record last-update time, timezone-aware UTC (T7).",
        "expire_at": "Absolute expiry instant for the lease term (T1); "
                     "readers filter lazily.",
    },
    "AuditEvent": {
        "event_id": "Idempotency key of this event (T4).",
        "task_id": "Associated task identifier.",
        "scope": "Free-form scope tag (business-defined).",
        "event_type": "Opaque event type string (T6).",
        "source": "Originating framework or producer (opaque).",
        "payload": "Free-form event payload (business-defined).",
        "created_at": "Event occurrence time, timezone-aware UTC (T7).",
    },
    "TaskPointer": {
        "backend": "Opaque storage backend identifier (T6).",
        "key": "Backend-specific addressing key (path, row id, vector id, ...).",
        "metadata": "Optional free-form metadata (content_type, size, ...).",
    },
    "Page": {
        "limit": "Maximum number of records to return (> 0).",
        "offset": "Number of records to skip (>= 0).",
    },
    "Sort": {
        "field": "Mechanism field to sort by; must be within the contract "
                 "whitelist for the queried domain.",
        "direction": "Sort direction (asc / desc).",
    },
    "SortDirection": {
        "__doc__": "Sort direction for query results.",
    },
    "TimeRange": {
        "start": "Inclusive range start, timezone-aware UTC (None = unbounded).",
        "end": "Exclusive range end, timezone-aware UTC (None = unbounded).",
    },
    "CapabilitySet": {
        "content_sink": "Declares ContentWriter implementation (T8).",
        "content_query": "Declares ContentReader implementation (T8).",
        "audit_sink": "Declares AuditWriter implementation (T8).",
        "audit_query": "Declares AuditReader implementation (T8).",
        "result_sink": "Declares ResultWriter implementation (T8).",
        "result_query": "Declares ResultReader implementation (T8).",
        "snapshot_sink": "Declares SnapshotWriter implementation (T8).",
        "snapshot_query": "Declares SnapshotReader implementation (T8).",
        "dependency_sink": "Declares DependencyWriter implementation (T8).",
        "dependency_query": "Declares DependencyReader implementation (T8).",
        "protocol_compat": "PEP 440 version specifier of the protocol "
                           "compatibility range this implementation supports.",
        "concurrency_domain": "Scope within which the adapter's atomicity "
                              "guarantees hold (T10): process / database / "
                              "distributed.",
    },
    "DependencyEdge": {
        "child_id": "Dependent task id (opaque reference, T6/T12).",
        "parent_id": "Dependency task id. Binds the task, not an execution "
                     "generation (T12).",
        "is_primary": "Primary-parent flag: the single chain used for "
                      "lineage and exemption inheritance.",
        "registered_at": "Edge registration instant, timezone-aware UTC (T7).",
    },
    "DependencyGraph": {
        "root_task_id": "The root this neighbourhood was read from.",
        "task_ids": "All reachable task identifiers (sorted, ids only — "
                    "pure-edge discipline, T12).",
        "edges": "All edges whose child_id is within the closure.",
    },
}

#: Artifacts. Single-model artifacts carry the model as root; "defs" entries
#: are definition libraries without a root type. Enums are not pydantic
#: models — their schemas come from _enum_schema().
_TARGETS: dict[str, dict[str, object]] = {
    "snapshot/0.1.json": {"kind": "model", "model": TaskSnapshot},
    "audit/0.1.json": {"kind": "model", "model": AuditEvent},
    "pointer/0.1.json": {"kind": "model", "model": TaskPointer},
    "query/0.1.json": {
        "kind": "defs",
        "models": [Page, Sort, TimeRange],
        "enums": [SortDirection],
    },
    "capabilities/0.1.json": {"kind": "model", "model": CapabilitySet},
    "dependency/0.1.json": {
        "kind": "defs",
        "models": [DependencyEdge, DependencyGraph],
    },
}


def _collapse_nullable(schema: dict, model_name: str) -> None:
    """Fold `anyOf: [T, null]` into `T` (wire-format: omit, never null)."""
    defs = schema.get("$defs", {})
    for model_schema in [schema, *defs.values()]:
        title = model_schema.get("title", model_name)
        field_descs = DESCRIPTIONS.get(title, {})
        for name, prop in model_schema.get("properties", {}).items():
            any_of = prop.get("anyOf")
            if (
                isinstance(any_of, list)
                and len(any_of) == 2
                and {"type": "null"} in any_of
            ):
                real = next(b for b in any_of if b != {"type": "null"})
                desc = prop.get("description", "")
                prop.clear()
                prop.update(real)
                base_desc = desc or field_descs.get(name, "")
                prop["description"] = (
                    f"{base_desc} Omitted when None.".strip()
                )


def _inject_datetime_pattern(schema: dict) -> None:
    """Enforce explicit timezone offsets on date-time fields (T7)."""
    for model_schema in [schema, *schema.get("$defs", {}).values()]:
        for prop in model_schema.get("properties", {}).values():
            if prop.get("format") == "date-time" and prop.get("type") == "string":
                prop["pattern"] = _EXPLICIT_OFFSET_PATTERN


def _inject_descriptions(schema: dict) -> None:
    """Apply DESCRIPTIONS to fields lacking a description."""
    for model_schema in [schema, *schema.get("$defs", {}).values()]:
        field_descs = DESCRIPTIONS.get(model_schema.get("title", ""), {})
        for name, prop in model_schema.get("properties", {}).items():
            if "description" not in prop and name in field_descs:
                prop["description"] = field_descs[name]


def _strip_defaults(schema: dict) -> None:
    """Remove leftover `"default": null` contradicted by omit-on-None."""
    for model_schema in [schema, *schema.get("$defs", {}).values()]:
        for prop in model_schema.get("properties", {}).values():
            if prop.get("default") is None and "default" in prop:
                del prop["default"]

def _enum_schema(enum_cls: type) -> dict:
    """JSON Schema for a str-Enum (frozen vocabulary, not a pydantic model)."""
    values = [member.value for member in enum_cls]
    schema: dict[str, object] = {
        "title": enum_cls.__name__,
        "type": "string",
        "enum": values,
    }
    desc = DESCRIPTIONS.get(enum_cls.__name__, {}).get("__doc__", "")
    if desc:
        schema["description"] = desc
    return schema

def _build_artifact(rel_id: str, spec: dict[str, object]) -> dict:
    """Build one artifact (export + post-processing + metadata injection)."""
    if spec["kind"] == "model":
        model: type[BaseModel] = spec["model"]  # type: ignore[assignment]
        title = model.__name__
        body = model.model_json_schema()
        artifact = {
            "$schema": _JSON_SCHEMA_DIALECT,
            "$id": f"{_ID_BASE}/{rel_id}",
            "title": title,
            "x-stability": _STABILITY,
            **body,
        }
    else:
        models: list[type[BaseModel]] = spec["models"]  # type: ignore[assignment]
        enums: list[type] = spec.get("enums", [])  # type: ignore[assignment]
        title = "QueryModels"
        defs: dict[str, dict] = {
            m.__name__: m.model_json_schema() for m in models
        }
        for enum_cls in enums:
            defs[enum_cls.__name__] = _enum_schema(enum_cls)
        artifact = {
            "$schema": _JSON_SCHEMA_DIALECT,
            "$id": f"{_ID_BASE}/{rel_id}",
            "title": title,
            "x-stability": _STABILITY,
            "$defs": defs,
        }
        # Inline single-model artifacts keep nested $defs; a defs library
        # flattens nested definitions one level up when present.
        for name, sub in list(artifact["$defs"].items()):
            nested = sub.pop("$defs", None)
            if nested:
                artifact["$defs"].update(nested)

    _collapse_nullable(artifact, title)
    _inject_datetime_pattern(artifact)
    _inject_descriptions(artifact)
    _strip_defaults(artifact)
    return artifact

def _render(artifact: dict) -> str:
    """Byte-deterministic rendering (drift gate diffs this verbatim)."""
    return json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def generate_all() -> dict[str, str]:
    """Return {relative_artifact_path: rendered_text} for all targets."""
    return {
        rel_id: _render(_build_artifact(rel_id, spec))
        for rel_id, spec in _TARGETS.items()
    }


def main() -> int:
    check = "--check" in sys.argv
    artifacts = generate_all()

    if check:
        drifted: list[str] = []
        for rel_id, rendered in artifacts.items():
            path = SCHEMA_DIR / rel_id
            if not path.is_file() or path.read_text(encoding="utf-8") != rendered:
                drifted.append(rel_id)
        if drifted:
            print("schema drift detected:")
            for rel_id in drifted:
                print(f"  - schemas/{rel_id}")
            print("run: python packages/protocol/scripts/generate_schemas.py")
            return 1
        print(f"schema artifacts up to date: {len(artifacts)} files")
        return 0

    for rel_id, rendered in artifacts.items():
        path = SCHEMA_DIR / rel_id
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        print(f"wrote schemas/{rel_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())