"""
The compatibility test of #1: the same spans go through the reference `OTLPSpanExporter` and through
`OTLPSpanProcessor`, and the fake receiver decodes equal requests.
"""

import math
from collections.abc import Callable, Sequence
from typing import Any, TypeVar, overload

import pytest
from fastotel import OTLPSpanProcessor
from google.protobuf import text_format
from google.protobuf.internal import api_implementation
from google.protobuf.message import Message
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanLimits, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExportResult
from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, SpanKind, Status, StatusCode, TraceFlags
from opentelemetry.trace.span import TraceState
from receiver import FakeReceiver
from spans import INT64_MAX, INT64_MIN, SAMPLED, end_every_kind_of_span, tracer

_T = TypeVar("_T")
# A ScopeSpans as compared: the scope, its schema URL and the spans, sorted
Scope = tuple[str, list[str]]
# A ResourceSpans as compared: the resource with its schema URL, and its scopes in order
Grouped = list[tuple[str, list[Scope]]]


class _Collect(SpanProcessor):
    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def on_end(self, span: ReadableSpan) -> None:
        # What BatchSpanProcessor hands the exporter
        if span.context is not None and span.context.trace_flags.sampled:
            self.spans.append(span)


def _text(message: Message) -> str:
    # The text format compares NaN equal to itself and shows a readable diff
    return text_format.MessageToString(message)


def grouped(requests: list[ExportTraceServiceRequest]) -> Grouped:
    """
    The resources, scopes and spans of `requests` as one request, the spans of each scope sorted. Groups merge
    with an equal one of an earlier request, since batch boundaries may differ, but not within a request: there
    two equal groups are a difference in grouping.
    """
    resources: Grouped = []
    for request in requests:
        merged_resources: list[int] = []
        for resource_spans in request.resource_spans:
            resource = f"{_text(resource_spans.resource)}schema_url: {resource_spans.schema_url!r}"
            scopes = _group(resources, resource, merged_resources)
            merged_scopes: list[int] = []
            for scope_spans in resource_spans.scope_spans:
                scope = f"{_text(scope_spans.scope)}schema_url: {scope_spans.schema_url!r}"
                _group(scopes, scope, merged_scopes).extend(_text(span) for span in scope_spans.spans)
    return [(resource, [(scope, sorted(spans)) for scope, spans in scopes]) for resource, scopes in resources]


def _group(groups: list[tuple[str, list[_T]]], key: str, taken: list[int]) -> list[_T]:
    """
    The values of the group `key` that no group of the same request has taken yet, a new group if there is none.
    """
    index = next((i for i, (k, _) in enumerate(groups) if k == key and i not in taken), len(groups))
    if index == len(groups):
        groups.append((key, []))
    taken.append(index)
    return groups[index][1]


def through_both(end_spans: Callable[[SpanProcessor, SpanProcessor], object]) -> tuple[Grouped, Grouped]:
    """
    What the fake receiver decodes of the spans `end_spans` ends into both processors: from fastotel and from
    the reference exporter, each sent in one batch.
    """
    with FakeReceiver() as ours, FakeReceiver() as theirs:
        processor = OTLPSpanProcessor(endpoint=ours.endpoint)
        collected = _Collect()
        end_spans(processor, collected)
        processor.shutdown()
        exporter = OTLPSpanExporter(endpoint=theirs.endpoint)
        assert exporter.export(collected.spans) == SpanExportResult.SUCCESS
        exporter.shutdown()  # type: ignore[no-untyped-call, unused-ignore]
        return grouped(ours.requests), grouped(theirs.requests)


def test_every_field_is_sent_as_the_reference_sends_it() -> None:
    ended: list[int] = []
    fastotel, reference = through_both(lambda *processors: ended.append(end_every_kind_of_span(*processors)))

    assert fastotel == reference
    # Every span is in the comparison, none was lost on either side
    assert sum(len(spans) for _, scopes in reference for _, spans in scopes) == ended[0]
    # Equal resources of two providers are one, the other is the one with limits
    assert len(reference) == 2


def test_the_comparison_merges_batches_and_tells_scopes_apart() -> None:
    request = ExportTraceServiceRequest()
    request.resource_spans.add().scope_spans.add().spans.add(name="a")
    other = ExportTraceServiceRequest()
    other.resource_spans.add().scope_spans.add(schema_url="other").spans.add(name="a")

    [(_, [(_, spans)])] = grouped([request, request])
    assert spans == ['name: "a"\n'] * 2
    assert grouped([request]) != grouped([other])
    split = ExportTraceServiceRequest()
    split.resource_spans.add().scope_spans.add().spans.add(name="a")
    split.resource_spans.add().scope_spans.add().spans.add(name="a")
    assert len(grouped([split])) == 2


def _all_text(requests: Grouped) -> str:
    return "".join(
        resource + "".join(scope + "".join(spans) for scope, spans in scopes) for resource, scopes in requests
    )


class _Unknown:
    """
    A type OTLP has no value for, with no `__str__` of its own, so the SDK would replace it with None.
    """


class _Unreadable(Sequence[int]):
    """
    A sequence whose items cannot be read.
    """

    def __len__(self) -> int:
        return 1

    @overload
    def __getitem__(self, index: int) -> int: ...
    @overload
    def __getitem__(self, index: slice) -> Sequence[int]: ...
    def __getitem__(self, index: int | slice) -> int | Sequence[int]:
        raise RuntimeError("unreadable")


def _self_referencing() -> list[Any]:
    value: list[Any] = [1]
    value.append(value)
    return value


def _nested(depth: int, leaf: Any = 1) -> Any:
    value = leaf
    for _ in range(depth):
        value = (value,)
    return value


def _nested_mapping(depth: int, leaf: Any) -> Any:
    value = leaf
    for _ in range(depth):
        value = {"k": value}
    return value


# Values the SDK lets through but OTLP cannot express; the reference leaves out the attribute, and so does fastotel
UNEXPRESSIBLE = {
    "int above int64": INT64_MAX + 1,
    "int below int64": INT64_MIN - 1,
    "int beyond int64 in a sequence": (1, INT64_MAX + 1),
    "int beyond int64 in a mapping": {"inner": {"deep": 2**64}},
    "str with a lone surrogate": "a\ud800b",
    "str with a lone surrogate in a sequence": ("ok", "\udfff"),
    "key with a lone surrogate in a mapping": {"\ud800": 1},
}
_UPB = api_implementation.Type() == "upb"
_UNEXPRESSIBLE = [
    *(pytest.param(value, id=case) for case, value in UNEXPRESSIBLE.items()),
    # With upb the reference cannot build a message nested more than 100 deep and leaves the attribute out; a bit
    # less deep, it loses the batch or sends what decoders refuse. Pure-Python protobuf sends it whatever the depth
    pytest.param(
        _nested(60),
        id="nested deeper than protobuf decoders accept",
        marks=pytest.mark.skipif(not _UPB, reason="the reference sends it on pure-Python protobuf"),
    ),
]


@pytest.mark.parametrize("value", _UNEXPRESSIBLE)
def test_an_attribute_otlp_cannot_express_is_left_out_as_by_the_reference(value: Any) -> None:
    def end_spans(*processors: SpanProcessor) -> None:
        provider = TracerProvider(resource=Resource({"service.name": "app", "odd": value}))
        for processor in processors:
            provider.add_span_processor(processor)
        app = tracer(provider, "app", attributes={"odd": value, "kept": 1})
        with app.start_as_current_span("span", attributes={"before": 1, "odd": value, "after": 2}) as span:
            span.add_event("event", {"odd": value, "kept": 1})
        app.start_span("linked", links=[Link(span.get_span_context(), {"odd": value, "kept": 1})]).end()

    fastotel, reference = through_both(end_spans)

    assert fastotel == reference
    assert '"odd"' not in _all_text(fastotel)
    assert '"after"' in _all_text(fastotel)


def test_an_attribute_is_left_out_when_the_request_would_nest_deeper_than_decoders_accept(
    receiver: FakeReceiver,
) -> None:
    # Protobuf decoders refuse more than 100 levels of messages; a span attribute's AnyValue is the 6th level of
    # the request, an array adds 2 (ArrayValue, AnyValue) and a mapping 3 (KeyValueList, KeyValue, AnyValue)
    attributes = {
        "array, its leaf at 100": _nested(47),
        "array, its leaf at 102": _nested(48),
        "empty array at 99": _nested(46, ()),
        "empty array at 101": _nested(47, ()),
        "mapping, its leaf at 99": _nested_mapping(31, 1),
        "mapping, its leaf at 102": _nested_mapping(32, 1),
        "empty mapping at 100": _nested_mapping(31, {}),
        "empty mapping at 103": _nested_mapping(32, {}),
    }
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    provider.get_tracer("app").start_span("deep", attributes=attributes).end()
    processor.shutdown()

    [span] = receiver.spans()
    kept = [key for key in attributes if key.endswith(("at 99", "at 100"))]
    assert [key_value.key for key_value in span.attributes] == kept


# Attributes a ReadableSpan can hold when it is built by hand, without the SDK's cleaning
UNCLEANED: dict[str, tuple[Any, Any]] = {
    "a type OTLP has no value for": ("odd", _Unknown()),
    "a type OTLP has no value for, in a sequence": ("odd", (1, _Unknown())),
    "a self-referencing list": ("odd", _self_referencing()),
    "a sequence whose iteration fails": ("odd", _Unreadable()),
    "a key that is not a str": (1, "odd"),
}


@pytest.mark.parametrize("attribute", UNCLEANED.values(), ids=UNCLEANED.keys())
def test_an_attribute_the_sdk_would_have_cleaned_is_left_out_as_by_the_reference(attribute: tuple[Any, Any]) -> None:
    key, value = attribute
    span = ReadableSpan(
        name="by hand",
        context=SpanContext(trace_id=1, span_id=2, is_remote=False, trace_flags=SAMPLED),
        resource=Resource({}),
        attributes={"before": 1, key: value, "after": 2},
        start_time=1,
        end_time=2,
    )

    def end_spans(*processors: SpanProcessor) -> None:
        for processor in processors:
            processor.on_end(span)

    fastotel, reference = through_both(end_spans)

    assert fastotel == reference
    assert '"odd"' not in _all_text(fastotel)
    assert '"after"' in _all_text(fastotel)


# Names protobuf cannot take for a string field; the reference then loses the batch, fastotel only the span
UNENCODABLE_NAMES = {
    "a lone surrogate": "\ud800",
    "bytes that are not UTF-8": b"\xff",
    "neither str, bytes nor None": 1,
}


@pytest.mark.parametrize("name", UNENCODABLE_NAMES.values(), ids=UNENCODABLE_NAMES.keys())
def test_a_span_with_a_name_protobuf_cannot_take_is_dropped_alone(name: Any) -> None:
    # The reference's encoder raises for the whole request and the exporter loses the batch (ADR 0003)
    def end_spans(*processors: SpanProcessor) -> None:
        provider = TracerProvider()
        for processor in processors:
            provider.add_span_processor(processor)
        provider.get_tracer("app").start_span("before").end()
        provider.get_tracer("app").start_span(name).end()
        provider.get_tracer("app").start_span("after").end()

    with FakeReceiver() as ours, FakeReceiver() as theirs:
        processor = OTLPSpanProcessor(endpoint=ours.endpoint)
        collected = _Collect()
        end_spans(processor, collected)
        processor.shutdown()
        exporter = OTLPSpanExporter(endpoint=theirs.endpoint)
        assert exporter.export(collected.spans) == SpanExportResult.FAILURE

        assert [span.name for span in ours.spans()] == ["before", "after"]
        assert theirs.received == []


def test_names_and_keys_of_none_or_bytes_are_sent_as_protobuf_takes_them() -> None:
    # The SDK does not check the type of names; protobuf sends None as empty and decodes bytes as UTF-8
    def end_spans(*processors: SpanProcessor) -> None:
        provider = TracerProvider()
        for processor in processors:
            provider.add_span_processor(processor)
        app = provider.get_tracer("app")
        for name in (None, b"bytes"):
            span = app.start_span(name)  # type: ignore[arg-type]
            span.add_event(name)  # type: ignore[arg-type]
            span.end()
        by_hand = ReadableSpan(
            name="by hand",
            context=SpanContext(trace_id=1, span_id=2, is_remote=False, trace_flags=SAMPLED),
            resource=Resource({}),
            attributes={b"bytes": 1},  # type: ignore[dict-item]
            start_time=1,
            end_time=2,
        )
        for processor in processors:
            processor.on_end(by_hand)

    fastotel, reference = through_both(end_spans)

    assert fastotel == reference
    assert _all_text(fastotel).count('"bytes"') == 3


# Strings the encoders must agree on, non-ASCII included; lone surrogates only where the reference drops an
# attribute rather than its batch
_TEXT = st.text(st.characters(exclude_categories=["Cs"]), max_size=8)
_ANY_TEXT = st.text(st.characters(exclude_categories=["Cs"]) | st.characters(categories=["Cs"]), max_size=8)
_KEYS = _ANY_TEXT.filter(bool)
_SCALARS = (
    st.none()
    | st.booleans()
    | st.integers(INT64_MIN, INT64_MAX)
    | st.integers()
    | st.floats()
    | st.sampled_from([math.nan, math.inf, -math.inf, -0.0])
    | _ANY_TEXT
    | st.binary(max_size=8)
)
_VALUES = st.recursive(
    _SCALARS,
    lambda children: (
        st.lists(children, max_size=4).map(tuple)
        | st.lists(children, max_size=4)
        | st.dictionaries(_ANY_TEXT, children, max_size=4)
    ),
    max_leaves=10,
)
# Homogeneous sequences are the attributes the API documents; generated apart so they come up often
_HOMOGENEOUS = st.one_of(
    st.lists(st.booleans(), max_size=4),
    st.lists(st.integers(INT64_MIN, INT64_MAX), max_size=4),
    st.lists(st.floats(), max_size=4),
    st.lists(_TEXT, max_size=4),
).map(tuple)
_ATTRIBUTES = st.dictionaries(_KEYS, _VALUES | _HOMOGENEOUS, max_size=6)
_IDS = st.integers(1, 2**64 - 1)
_CONTEXTS = st.builds(
    SpanContext,
    trace_id=st.integers(1, 2**128 - 1),
    span_id=_IDS,
    is_remote=st.booleans(),
    trace_flags=st.sampled_from([SAMPLED, TraceFlags(0)]),
    trace_state=st.lists(
        st.tuples(
            st.from_regex(r"[a-z][a-z0-9_*/-]{0,8}", fullmatch=True),
            st.from_regex(r"[!-+\--<>-~]{1,8}", fullmatch=True),
        ),
        max_size=3,
        unique_by=lambda pair: pair[0],
    ).map(TraceState),
)


@st.composite
def _span_shapes(draw: st.DrawFn) -> dict[str, Any]:
    return {
        "name": draw(_TEXT),
        "kind": draw(st.sampled_from(SpanKind)),
        "parent": draw(st.none() | st.just("local") | _CONTEXTS.filter(lambda context: context.trace_flags.sampled)),
        "attributes": draw(_ATTRIBUTES),
        "events": draw(st.lists(st.tuples(_TEXT, _ATTRIBUTES, st.integers(0, 2**64 - 1)), max_size=3)),
        "links": draw(st.lists(st.tuples(_CONTEXTS, _ATTRIBUTES), max_size=3)),
        "status": draw(
            st.sampled_from([Status(StatusCode.UNSET), Status(StatusCode.OK)])
            | st.builds(Status, st.just(StatusCode.ERROR), _TEXT)
        ),
        "scope": draw(st.sampled_from(["a", "b"])),
    }


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    resource=_ATTRIBUTES,
    scope=st.tuples(st.none() | _TEXT, st.none() | _TEXT, _ATTRIBUTES),
    limits=st.builds(
        SpanLimits,
        max_span_attributes=st.none() | st.integers(0, 4),
        max_events=st.none() | st.integers(0, 2),
        max_links=st.none() | st.integers(0, 2),
        max_event_attributes=st.none() | st.integers(0, 2),
        max_link_attributes=st.none() | st.integers(0, 2),
    ),
    shapes=st.lists(_span_shapes(), min_size=1, max_size=5),
)
def test_generated_spans_are_sent_as_the_reference_sends_them(
    resource: dict[str, Any],
    scope: tuple[str | None, str | None, dict[str, Any]],
    limits: SpanLimits,
    shapes: list[dict[str, Any]],
) -> None:
    version, schema_url, scope_attributes = scope

    def end_spans(*processors: SpanProcessor) -> None:
        provider = TracerProvider(resource=Resource(resource, schema_url), span_limits=limits)
        for processor in processors:
            provider.add_span_processor(processor)
        tracers = {
            "a": tracer(provider, "a", version, schema_url, scope_attributes),
            "b": tracer(provider, "b"),
        }
        with tracers["a"].start_as_current_span("local parent") as local:
            for shape in shapes:
                parent = shape["parent"]
                if parent is None:
                    context = trace.set_span_in_context(trace.INVALID_SPAN)
                elif parent == "local":
                    context = trace.set_span_in_context(local)
                else:
                    context = trace.set_span_in_context(NonRecordingSpan(parent))
                span = tracers[shape["scope"]].start_span(
                    shape["name"],
                    context=context,
                    kind=shape["kind"],
                    attributes=shape["attributes"],
                    links=[Link(link_context, attributes) for link_context, attributes in shape["links"]],
                )
                for name, attributes, timestamp in shape["events"]:
                    span.add_event(name, attributes, timestamp)
                span.set_status(shape["status"])
                span.end()

    fastotel, reference = through_both(end_spans)

    assert fastotel == reference
