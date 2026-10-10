"""
The tracing setups the benchmarks compare.

A scenario takes the base URL of the local OTLP sink and returns a tracer provider plus the function that shuts it
down. Adding one, such as fastotel's processor (#18), is one more entry in SCENARIOS; the others stay as they are.
"""

from collections.abc import Callable, Sequence
from typing import NamedTuple

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import NoOpTracerProvider, TracerProvider

from bench.sink import TRACES_PATH


class Setup(NamedTuple):
    provider: TracerProvider
    shutdown: Callable[[], None]


class Scenario(NamedTuple):
    title: str
    setup: Callable[[str], Setup]


class _DiscardingExporter(SpanExporter):
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return SpanExportResult.SUCCESS


def _api(sink: str) -> Setup:
    # The tracer that the API's proxy hands every call to in an application without the SDK
    return Setup(NoOpTracerProvider(), lambda: None)


def _sdk(exporter: SpanExporter) -> Setup:
    # The defaults of the SDK, as a service gets them without any OTEL_BSP_* variable
    provider = SdkTracerProvider()
    provider.add_span_processor(BatchSpanProcessor(exporter))
    return Setup(provider, provider.shutdown)


def _sdk_discarding(sink: str) -> Setup:
    return _sdk(_DiscardingExporter())


def _sdk_otlp_http(sink: str) -> Setup:
    return _sdk(OTLPSpanExporter(endpoint=sink + TRACES_PATH))


SCENARIOS: dict[str, Scenario] = {
    "api": Scenario("API, no SDK", _api),
    "sdk": Scenario("SDK + BatchSpanProcessor, no-op exporter", _sdk_discarding),
    "sdk-otlp": Scenario("SDK + BatchSpanProcessor + OTLP/HTTP exporter", _sdk_otlp_http),
}
# Tracing off: the costs of the others are measured against it
BASELINE = "api"
