# orditect-bridge-openai Changelog

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