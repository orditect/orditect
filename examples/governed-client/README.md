# Orditect Governed-Client Example

Wrap **any existing callable** in Orditect governance — semaphore,
budget, audit, and content pointer-ization — without building a
workflow. This is the community integration entry point: how to embed
Orditect into code you already have.

Covers: `GovernedClient` (non-streaming) and `GovernedCallClient`
(non-streaming + streaming), with `cost_fn` pricing, `call_id`
dual-habitat idempotency, the streaming semaphore lifecycle, and the
usage-missing (A5) pricing path. Zero infrastructure (in-memory
doubles, mock LLM).

## Run

    pip install -r requirements.txt
    python run_demo.py

For workflow-level governance (orchestrator, recovery, HITL), see
`examples/mvp`. For the full guided tour, see `examples/README.md` Ch.4.4.