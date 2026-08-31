"""Pretty-print the StreamRunner event stream as readable SSE frames.

The demo prints the protocol events instead of rendering them — proving
the protocol is correct. Rendering (React/Vue/SSE client) is the
developer's own product layer, per the adapter-ui discipline.
"""

from __future__ import annotations

from orditect.stream.events import EventType


def make_printer(verbose_delta: bool = True):
    """Return an async on_event(envelope, event_type) callback."""

    async def on_event(envelope, event_type) -> None:
        et = event_type
        data = envelope.data
        sid = envelope.stream_id
        seq = envelope.seq
        if et is EventType.STREAM_START:
            print(f"\n[SSE {sid} #{seq}] stream.start stages={data.get('stages')}")
        elif et is EventType.STREAM_DELTA:
            if verbose_delta:
                kind = data.get("kind")
                kind_str = kind.value if hasattr(kind, "value") else str(kind)
                text = (data.get("text") or "").replace("\n", "\\n")
                print(f"[SSE {sid} #{seq}] delta.{kind_str}: {text}")
        elif et is EventType.ENRICH_MARKER:
            print(f"[SSE {sid} #{seq}] enrich.marker id={data.get('placeholder_id')}")
        elif et is EventType.ENRICH_PLACEHOLDER:
            print(f"[SSE {sid} #{seq}] enrich.placeholder id={data.get('placeholder_id')} "
                  f"loading={data.get('loading_url')}")
        elif et is EventType.ENRICH_RESOLVED:
            print(f"[SSE {sid} #{seq}] enrich.resolved id={data.get('placeholder_id')} "
                  f"url={data.get('url')}")
        elif et is EventType.STAGE_END:
            print(f"[SSE {sid} #{seq}] stage.end name={data.get('name')}")
        elif et is EventType.STREAM_MANIFEST:
            phs = data.get("placeholders", [])
            print(f"[SSE {sid} #{seq}] stream.manifest placeholders={len(phs)}")
        elif et is EventType.STREAM_CANCELLED:
            partial = (data.get("partial_content") or "")[:60]
            print(f"[SSE {sid} #{seq}] stream.cancelled reason={data.get('reason')} "
                  f"partial={partial!r}")
        elif et is EventType.STREAM_END:
            print(f"[SSE {sid} #{seq}] stream.end")

    return on_event