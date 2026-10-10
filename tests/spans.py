"""
Spans that set every field OTLP has, for the compatibility tests: every attribute type on the span, the resource,
the scope, events and links, every kind and status, remote and local parents, dropped counts, and odd but legal
values. Only the SDK is needed, so the lowest job of CI can send them too.
"""

import math
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, SpanLimits, SpanProcessor, TracerProvider
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, SpanKind, Status, StatusCode, TraceFlags, Tracer
from opentelemetry.trace.span import TraceState

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

# Values of every type the SDK keeps, at the edges where encoders tend to differ
ATTRIBUTES: dict[str, Any] = {
    "bool": True,
    "false": False,
    "int": 42,
    "int.min": INT64_MIN,
    "int.max": INT64_MAX,
    "zero": 0,
    "float": 0.25,
    "float.negative.zero": -0.0,
    "nan": math.nan,
    "inf": math.inf,
    "-inf": -math.inf,
    "str": "value",
    "str.empty": "",
    "str.non-ascii": "Grüße, 世界 🌍",
    "bools": (True, False),
    "ints": (1, INT64_MIN, INT64_MAX),
    "floats": (1.5, math.nan, -math.inf),
    "strs": ("a", "", "é"),
    "empty": (),
    # The extended attributes of newer SDKs; older ones drop them when the span sets them
    "none": None,
    "bytes": b"\x00\xff",
    "mixed": (1, "two", 3.0, None, True),
    "nested": ((1, 2), ("a",), ()),
    "mapping": {"key": "value", "inner": {"deep": (1, 2)}, "empty": {}},
    "ключ": "значение",
}

SAMPLED = TraceFlags(TraceFlags.SAMPLED)
REMOTE_PARENT = SpanContext(
    trace_id=0x5B8EFFF798038103D269B633813FC60C,
    span_id=0xEEE19B7EC3C1B174,
    is_remote=True,
    trace_flags=SAMPLED,
    trace_state=TraceState([("vendor", "opaque-1"), ("other", "k:v")]),
)
LINKED_REMOTE = SpanContext(trace_id=1, span_id=2, is_remote=True, trace_flags=SAMPLED)
LINKED_LOCAL = SpanContext(trace_id=INT64_MAX, span_id=3, is_remote=False, trace_flags=TraceFlags(0))


def tracer(
    provider: TracerProvider,
    name: str,
    version: str | None = None,
    schema_url: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Tracer:
    try:
        return provider.get_tracer(name, version, schema_url, attributes)  # type: ignore[call-arg, unused-ignore]
    except TypeError:
        # Scopes have attributes from SDK 1.26
        return provider.get_tracer(name, version, schema_url)


def provider(resource: Resource, processors: tuple[SpanProcessor, ...], **limits: int) -> TracerProvider:
    created = TracerProvider(resource=resource, span_limits=SpanLimits(**limits))
    for processor in processors:
        created.add_span_processor(processor)
    return created


def end_every_kind_of_span(*processors: SpanProcessor) -> int:
    """
    Ends spans that set every field into each of `processors`; returns how many.
    """
    checkout = provider(
        Resource({"service.name": "checkout", "service.instance.id": "é-1", **ATTRIBUTES}, "https://example.com/1"),
        processors,
    )
    app = tracer(checkout, "app", "1.2.3", "https://example.com/2", {"team": "cart", **ATTRIBUTES})
    ended = 0

    with app.start_as_current_span("GET /cart", kind=SpanKind.SERVER, attributes=ATTRIBUTES) as root:
        root.add_event("cache miss", {"key": "cart:1", **ATTRIBUTES}, timestamp=1_700_000_000_000_000_000)
        root.add_event("")
        with app.start_as_current_span(
            "SELECT cart",
            kind=SpanKind.CLIENT,
            links=[Link(LINKED_REMOTE, {"kind": "follows", **ATTRIBUTES}), Link(LINKED_LOCAL)],
        ) as child:
            child.set_status(Status(StatusCode.ERROR, "timed out"))
        root.set_status(Status(StatusCode.OK))
        # An empty name, with no attributes at all
        app.start_span("").end()
        ended += 3

    # A remote parent: the flags say so, and the trace state comes from it
    remote = trace.set_span_in_context(NonRecordingSpan(REMOTE_PARENT))
    app.start_span("consume", context=remote, kind=SpanKind.CONSUMER).end()
    app.start_span("publish", kind=SpanKind.PRODUCER).end()
    ended += 2

    # Another tracer object for the same scope shares its ScopeSpans; another version is another scope
    tracer(checkout, "app", "1.2.3", "https://example.com/2", {"team": "cart", **ATTRIBUTES}).start_span("same").end()
    tracer(checkout, "app", "2.0").start_span("other version").end()
    # Scopes whose attributes differ only in order are equal for the SDK
    tracer(checkout, "ordered", attributes={"a": 1, "b": 2}).start_span("a, b").end()
    tracer(checkout, "ordered", attributes={"b": 2, "a": 1}).start_span("b, a").end()
    ended += 4

    # Limits make the SDK drop attributes, events and links, and count them
    payments = provider(
        Resource({"service.name": "payments"}),
        processors,
        max_span_attributes=2,
        max_events=1,
        max_links=1,
        max_event_attributes=1,
        max_link_attributes=1,
    )
    limited = payments.get_tracer("limited").start_span(
        "limited",
        attributes={"a": 1, "b": 2, "c": 3},
        # The SDK keeps the last ones
        links=[Link(LINKED_LOCAL), Link(LINKED_REMOTE, {"a": 1, "b": 2})],
    )
    limited.add_event("dropped")
    limited.add_event("kept", {"a": 1, "b": 2})
    limited.end()
    ended += 1

    # A span created without a tracer has no scope
    unscoped = ReadableSpan(
        name="no scope",
        context=SpanContext(trace_id=7, span_id=8, is_remote=False, trace_flags=SAMPLED),
        resource=checkout.resource,
        # SDK 1.16 cannot read the attributes of a span built without them
        attributes={},
        events=(Event("event", {"a": 1}, timestamp=5),),
        start_time=1,
        end_time=2,
    )
    for processor in processors:
        processor.on_end(unscoped)
    ended += 1
    return ended
