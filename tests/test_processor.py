import sys
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import fastotel
import pytest
from fastotel import OTLPSpanProcessor
from opentelemetry import trace
from opentelemetry.proto.common.v1.common_pb2 import KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span as SpanProto
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.sampling import Decision, Sampler, SamplingResult
from opentelemetry.util._once import Once
from receiver import FakeReceiver


@pytest.fixture
def global_provider() -> Iterator[None]:
    yield
    # The API lets a process set the global provider once; the SDK's own tests reset it the same way
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    trace._TRACER_PROVIDER = None


def string_attributes(key_values: Iterable[KeyValue]) -> dict[str, str]:
    return {key_value.key: key_value.value.string_value for key_value in key_values}


def fields(span: ReadableSpan) -> dict[str, Any]:
    """
    The fields of #7 as the SDK holds them, in the shape of a decoded span.
    """
    assert span.context is not None
    return {
        "trace_id": span.context.trace_id.to_bytes(16, "big"),
        "span_id": span.context.span_id.to_bytes(8, "big"),
        "parent_span_id": span.parent.span_id.to_bytes(8, "big") if span.parent else b"",
        "name": span.name,
        # OTLP numbers the kinds from SPAN_KIND_INTERNAL = 1, the API from INTERNAL = 0
        "kind": span.kind.value + 1,
        "start_time_unix_nano": span.start_time,
        "end_time_unix_nano": span.end_time,
        "attributes": dict(span.attributes or {}),
    }


def decoded_fields(span: SpanProto) -> dict[str, Any]:
    return {
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "parent_span_id": span.parent_span_id,
        "name": span.name,
        "kind": span.kind,
        "start_time_unix_nano": span.start_time_unix_nano,
        "end_time_unix_nano": span.end_time_unix_nano,
        "attributes": string_attributes(span.attributes),
    }


def test_snippet_delivers_the_fields_of_a_span(receiver: FakeReceiver, global_provider: None) -> None:
    # The snippet of #1, with the endpoint of the fake receiver
    provider = TracerProvider()
    provider.add_span_processor(OTLPSpanProcessor(endpoint=receiver.endpoint))
    trace.set_tracer_provider(provider)

    tracer = trace.get_tracer("checkout", "1.2.3")
    server = trace.SpanKind.SERVER
    with tracer.start_as_current_span("GET /cart", kind=server, attributes={"http.method": "GET"}) as parent:
        with tracer.start_as_current_span("SELECT cart") as child:
            child.set_attribute("db.system", "postgresql")
    assert provider.force_flush()

    assert isinstance(parent, ReadableSpan) and isinstance(child, ReadableSpan)
    [request] = receiver.requests
    [resource_spans] = request.resource_spans
    assert string_attributes(resource_spans.resource.attributes) == dict(provider.resource.attributes)
    [scope_spans] = resource_spans.scope_spans
    assert (scope_spans.scope.name, scope_spans.scope.version) == ("checkout", "1.2.3")
    # The child ends first
    assert [decoded_fields(span) for span in scope_spans.spans] == [fields(child), fields(parent)]
    provider.shutdown()


def test_posts_protobuf_to_the_endpoint(receiver: FakeReceiver) -> None:
    provider = TracerProvider()
    provider.add_span_processor(OTLPSpanProcessor(endpoint=receiver.endpoint))
    provider.get_tracer("app").start_span("span").end()
    provider.shutdown()

    [received] = receiver.received
    assert received.path == "/v1/traces"
    assert received.headers["content-type"] == "application/x-protobuf"


def test_groups_spans_by_resource_and_scope(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    providers = [TracerProvider(resource=Resource({"service.name": name})) for name in ("cart", "payments")]
    for provider in providers:
        provider.add_span_processor(processor)
    for tracer in (providers[0].get_tracer("a"), providers[0].get_tracer("b"), providers[1].get_tracer("a")):
        tracer.start_span("span").end()
    providers[0].get_tracer("a").start_span("span").end()
    assert processor.force_flush()

    [request] = receiver.requests
    grouped = [
        (
            string_attributes(resource_spans.resource.attributes)["service.name"],
            [(scope_spans.scope.name, len(scope_spans.spans)) for scope_spans in resource_spans.scope_spans],
        )
        for resource_spans in request.resource_spans
    ]
    assert grouped == [("cart", [("a", 2), ("b", 1)]), ("payments", [("a", 1)])]
    processor.shutdown()


class _RecordOnly(Sampler):
    def should_sample(self, *args: Any, **kwargs: Any) -> SamplingResult:
        return SamplingResult(Decision.RECORD_ONLY)

    def get_description(self) -> str:
        return "RecordOnly"


def test_exports_only_sampled_spans(receiver: FakeReceiver) -> None:
    # A span recorded but not sampled still reaches on_end; BatchSpanProcessor does not export it either
    provider = TracerProvider(sampler=_RecordOnly())
    provider.add_span_processor(OTLPSpanProcessor(endpoint=receiver.endpoint))
    provider.get_tracer("app").start_span("recorded").end()
    provider.shutdown()

    assert receiver.spans() == []


def test_force_flush_exports_what_is_queued(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    for _ in range(3):
        provider.get_tracer("app").start_span("span").end()

    assert processor.force_flush()
    assert len(receiver.spans()) == 3
    provider.shutdown()


def test_force_flush_and_shutdown_before_any_span(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)

    assert processor.force_flush()
    processor.shutdown()
    assert receiver.received == []


def test_shutdown_exports_what_is_queued_then_ignores_spans(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("app")
    tracer.start_span("before").end()

    processor.shutdown()
    tracer.start_span("after").end()
    processor.shutdown()  # a second call does nothing
    assert processor.force_flush()

    assert [span.name for span in receiver.spans()] == ["before"]


def _native_threads() -> list[str]:
    return [comm.read_text().strip() for comm in Path("/proc/self/task").glob("*/comm")]


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads the threads of the process from /proc")
def test_starts_no_thread_before_the_first_span(receiver: FakeReceiver) -> None:
    # A process that forks after creating the processor, as gunicorn --preload does, has no thread to lose
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    assert "fastotel-export" not in _native_threads()

    provider.get_tracer("app").start_span("span").end()
    assert "fastotel-export" in _native_threads()

    provider.shutdown()
    assert "fastotel-export" not in _native_threads()


def test_is_a_span_processor_exported_by_the_package() -> None:
    assert isinstance(OTLPSpanProcessor(), SpanProcessor)
    assert "OTLPSpanProcessor" in fastotel.__all__
