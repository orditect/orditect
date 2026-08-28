# orditect-bridge-openai Changelog

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