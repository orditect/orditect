# orditect-bridge-openai Changelog


## [0.1.7] - TBD

### Fixed
- Declared `orditect-stream>=0.1.6,<0.2` as a runtime dependency — the
  client imports `orditect.stream.protocols.source`; a standalone install
  previously broke on import. The import-boundary gate now cross-checks
  internal imports against pyproject to prevent recurrence.
- `GovernedLLMClient.stream`: the governed stream is now aclosed in
  finally (aclose cascade) — a consumer break deterministically releases
  the semaphore and closes the HTTP stream instead of relying on GC
  timing. New pin in `tests/test_client.py`.
### Fixed (tooling / hygiene)

- **meta: gate no-duplicates pin hardened** — the v0.1.6 pin
  re-implemented the aggregation loop in the test and never invoked the
  gate's real main(), so a reintroduced duplicate scan block would have
  stayed green. The gate now exposes `_scan_file()` as the single per-file
  scan path and the meta test drives the real main() against a temporary
  packages tree (each file's findings/advisory appear exactly once).
- **flow / stream test conftest**: sibling package src/ is now injected on
  a fresh clone (core / protocol for flow; core / flow / protocol /
  adapter-memory for stream), mirroring the bridge-openai / adapter-ui
  conftest pattern — `scripts/run_all_tests.sh` now works without
  pip-installing sibling packages.
- **core: `requirements.txt` floor aligned to pyproject**
  (orditect-protocol>=0.1.6; the floors meta test only reads pyproject).

### Changed (docs)

- `stream/adapters/taskflow.py`: `TaskflowResultStore` documents
  `manifest` as a reserved status word for its task records (callers must
  never drive status transitions on them — v0.1.6 CHANGELOG claim
  fulfilled).
- `flow/docs/governance.md`: `rebuild_dep_counters` `skipped_children`
  signals cold/hot data inconsistency — manual review and re-run required
  (v0.1.6 CHANGELOG claim fulfilled).
- Doc drift aligned with code reality: protocol README (twelve terms, 5
  domains, 10 protocols), ROADMAP status table, root README (eight
  packages, protocol 5域/10协议/12条款).


## [0.1.6] - TBD

### Fixed
- `_latency_ms` fully removed from the streaming path (C5 completion) —
  the result holder handed to cost_fn now carries only endpoint vocabulary
  (usage/model), never internal fields. New pin.

### Changed
- Dependency floors raised to >=0.1.6 (incl. test extra adapter-memory).


## [0.1.5] - TBD

### Fixed
- `_latency_ms` no longer leaks into the caller-visible provider response
  (latency is recorded by GovernedCallClient as `elapsed_ms`; the bridge
  no longer injects internal fields into the endpoint result). (2 FLIPs
  in tests)

### Changed
- Audit payloads now carry `cost_units` whenever cost_fn is evaluated
  (via the flow GovernedCallClient observability semantics).
- Dependency floor raised: orditect-protocol>=0.1.5, orditect-core>=0.1.5,
  orditect-flow>=0.1.5.

## [0.1.4] - TBD

### Fixed
- `stream()`: `result_fn` now returns the result holder only when usage
  is actually present; otherwise None (A5: usage-missing streams must
  hand None to cost_fn so the business prices it — the holder containing
  only `_latency_ms` no longer masquerades as a result).

### Tests
- Streaming tests now close the generator explicitly (`aclose()`), so the
  finally chain (charge + audit write) executes deterministically
  (previously timing-dependent on generator GC).

## [0.1.3] - TBD

### Added
- Initial release: OpenAI-compatible endpoint bridge (producer tier reference).
- GovernedLLMClient: governed LLM calls (semaphore + budget + audit +
  content pointer-ization) via GovernedCallClient composition.
- Two call forms:
  - chat(): non-streaming, returns endpoint result dict
  - stream(): streaming, yields orditect-stream SourceChunk (implements
    LLMSourceProtocol)
- Vocabulary discipline: OpenAI-shaped words (model/messages/usage/
  finish_reason) translated at bridge edge into audit payloads and cost
  dicts; never flows back into framework packages.
- cost_fn discipline: usage missing (usage=None) → business prices it;
  bridge never silently estimates.
- Passes producer conformance profile.