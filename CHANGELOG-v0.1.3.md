# Orditect v0.1.3 Changelog

**Version theme**: Three-category protocolization — core / adapters / bridges
communicate through protocols, not implementations.

## Added

### Core (framework)

- **GovernedCallClient** (`flow/governor/call.py`): the standard form of one
  governed call, composing GovernedClient (governance half) with observation
  (audit + content pointer-ization) and three opaque labels (task_id /
  parent_task_id / execution_id). Extension, not reinvention.
- **call_id dual-habitat idempotency**: hot path (quota already_reserved) and
  cold path (audit event_id) now share the same call_id — retry with the same
  call_id dedups at both layers.
- **ActionDispatcher** (`flow/actions/`): flow-side asynchronous executor for
  action commands (pause/retry/resume), consuming from an action queue and
  delegating to flow's public operation surface (orchestrator cancel /
  RecoveryService rerun/resume). Command-queue form per DD-013.

### Adapters

- **orditect-adapter-local**: local-file storage adapter (document-family
  reference). Five domains over plain files (ndjson envelope streams + JSON
  payloads), content-addressed blobs, write-atomic (tmp+rename). Passes full
  conformance profile. First producer of the trace-bundle data form.
- **orditect-adapter-ui**: UI adapter reference implementation. Consumer read
  (TraceBundleReader: parses trace bundles without importing orditect
  internals) + action sink (ActionSinkAdapter: writes action commands to a
  queue for HITL/MCP/agent intervention).

### Bridges

- **orditect-bridge-openai**: OpenAI-compatible endpoint bridge (reference
  implementation, producer tier). GovernedLLMClient wraps any OpenAI-compatible
  endpoint in the governed-call form (semaphore + budget + audit + content
  pointer-ization). Two call forms: non-streaming chat() and streaming
  stream() (implements LLMSourceProtocol for orditect-stream).

### Protocol & Gates

- **check_import_boundary.py**: new gate enforcing package dependency rules
  (no business imports, internal imports restricted to allowed_internal,
  third-party imports must be declared in pyproject.toml).
- Registered new packages in `scripts/gates/common.py`: adapter-local,
  bridge-openai, adapter-ui.

## Changed

- **Action sink form**: UI adapter's action channel changed from direct
  invocation to command-queue form (DD-013 bypass principle). Action commands
  are enqueued; flow's ActionDispatcher consumes and executes asynchronously.
  ActionCommand/ActionType/ActionQueue models live in flow (shared mechanism
  records).

## Verification

- adapter-local passes full conformance profile (five domains).
- bridge-openai passes producer conformance profile.
- adapter-ui passes consumer profile (TraceBundleReader) + action profile
  (ActionSinkAdapter + ActionDispatcher).
- End-to-end governance loop verified: bridge → core → storage → UI → action.
- run_rules validates trace bundles with zero violations.
- Swap tests: adapters interchangeable without changing bridges; bridges
  interchangeable without changing adapters.

## Documentation

- `docs/integration-guide.md`: three-category integration guide with
  certification checklist and boundary discipline.

## Freeze Criteria Progress

| Criterion | v0.1.2 | v0.1.3 |
|---|---|---|
| Relational backend (full profile) | ❌ | ❌ (PG, commercial layer) |
| Document backend (full profile) | ❌ | ✅ adapter-local |
| External producer (producer profile) | ❌ | ✅ bridge-openai |
| UI interaction (consumer + action) | ❌ | ✅ adapter-ui |

## Explicitly Not Done

- No concrete adapter/bridge productization (reference implementations only).
- No new governance mechanisms (all reuse existing core/flow assets).
- No business concepts in framework layers (OpenAI vocabulary stays in bridge).
- No UI frontend implementation (that's product layer).
- No apps / gateways / knowledge packs / clients / SDKs (outside framework).
- No framework graph-structure mapping spec (deferred to optional layer 2).
- No SaaS / hosted services.