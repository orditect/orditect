# Bridge discipline

Rules for bridge packages (orditect-bridge-*: LangGraph / LangChain /
DeepAgent / AutoGen / ...). Each rule names its enforcement body.

## 1. Translation at the edge; the framework stays ignorant

External-framework vocabulary (LangGraph node names, AutoGen roles,
DeepAgent terms) is translated to opaque strings **inside the bridge
package only**. Wrong: writing `status="langgraph_node_finished"` into a
snapshot — this does not violate T6's letter (the string is opaque) but
violates its spirit (an external vocabulary is being used as state-machine
language). Right: map to a vocabulary the bridge declares itself, and keep
the mapping table in the bridge package.

**Enforcement**: review + M0 gates (business-neutrality, import-boundary).

## 2. Minimum implementation path

`snapshot_sink` + `audit_sink` is the entry point (plus `dependency_sink`
when wiring dependency governance). Certify under the **producer** tier.

**Enforcement**: conformance suite (`profile="producer"`).

## 3. Vocabulary declaration duty

Every status/event_type word a bridge writes must be listed in the bridge
package's own documentation. The framework does not know your words — but
your users must (T6's bridge-side projection).

**Enforcement**: review.

## 4. Clock duty (T7, multi-producer)

A bridge writes timestamps with its own timezone-aware UTC clock.
Cross-producer comparisons are approximate semantics; `expire_at` is
authoritative as written by its producer.

**Enforcement**: data rules (DR-ALL-001 checks aware-ness).

## 5. No backflow

No bridge type, vocabulary, or helper may appear in the import graph or
string content of the five framework packages (core / flow / stream /
protocol / adapter-memory).

**Enforcement**: M0 gates — `check_import_boundary.py` (business import
blacklist) and `check_business_neutrality.py` (ecosystem vocabulary).