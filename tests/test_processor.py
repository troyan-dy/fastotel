import gc
import os
import signal
import sys
import time
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
from spans import end_every_kind_of_span


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


def test_sends_every_kind_of_span(receiver: FakeReceiver) -> None:
    # test_compatibility.py compares them with the reference; this runs in the lowest job too, with an old SDK
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    ended = end_every_kind_of_span(processor)
    processor.shutdown()

    assert len(receiver.spans()) == ended


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


def _export_threads() -> int:
    # Counted rather than looked for: a processor another test left running has a thread of the same name
    return [comm.read_text().strip() for comm in Path("/proc/self/task").glob("*/comm")].count("fastotel-export")


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads the threads of the process from /proc")
def test_starts_no_thread_before_the_first_span(receiver: FakeReceiver) -> None:
    # A process that forks after creating the processor, as gunicorn --preload does, has no thread to lose
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    before = _export_threads()
    provider.add_span_processor(processor)
    assert _export_threads() == before

    provider.get_tracer("app").start_span("span").end()
    assert _export_threads() == before + 1

    provider.shutdown()
    assert _export_threads() == before


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_a_child_forked_after_the_first_span_exits_cleanly(receiver: FakeReceiver) -> None:
    # The child does not export until #13; it must not crash or hang on the worker it did not inherit
    provider = TracerProvider()
    provider.add_span_processor(OTLPSpanProcessor(endpoint=receiver.endpoint))
    tracer = provider.get_tracer("app")
    tracer.start_span("parent before").end()

    pid = os.fork()
    if pid == 0:
        tracer.start_span("child").end()
        provider.force_flush()
        provider.shutdown()
        os._exit(0)
    deadline = time.monotonic() + 10
    while (waited := os.waitpid(pid, os.WNOHANG)) == (0, 0) and time.monotonic() < deadline:
        time.sleep(0.01)
    if waited == (0, 0):
        os.kill(pid, signal.SIGKILL)
        os.waitpid(pid, 0)
        pytest.fail("the child hung")
    assert os.waitstatus_to_exitcode(waited[1]) == 0

    tracer.start_span("parent after").end()
    provider.shutdown()
    assert [span.name for span in receiver.spans()] == ["parent before", "parent after"]


def test_splits_what_is_queued_into_batches_of_512(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("app")
    for _ in range(1300):
        tracer.start_span("span").end()
    provider.shutdown()

    sizes = [len(request.resource_spans[0].scope_spans[0].spans) for request in receiver.requests]
    assert sorted(sizes) == [276, 512, 512]


def test_a_processor_dropped_without_shutdown_exports_what_is_queued(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.on_end(_ended_span(TracerProvider()))
    del processor
    gc.collect()

    deadline = time.monotonic() + 10
    while not receiver.spans() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(receiver.spans()) == 1


def _ended_span(provider: TracerProvider) -> ReadableSpan:
    span = provider.get_tracer("app").start_span("span")
    span.end()
    assert isinstance(span, ReadableSpan)
    return span


def test_on_end_drops_what_it_cannot_copy_without_raising(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.on_end(object())  # type: ignore[arg-type]
    processor.on_end(_ended_span(TracerProvider()))
    processor.shutdown()

    assert len(receiver.spans()) == 1


def test_force_flush_with_a_negative_timeout_does_not_raise(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.force_flush(-1)
    processor.shutdown()


def test_is_a_span_processor_exported_by_the_package() -> None:
    assert isinstance(OTLPSpanProcessor(), SpanProcessor)
    assert "OTLPSpanProcessor" in fastotel.__all__
