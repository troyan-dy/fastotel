from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor

from fastotel import _fastotel

# Where the reference OTLPSpanExporter sends without configuration
DEFAULT_ENDPOINT = "http://localhost:4318/v1/traces"


class OTLPSpanProcessor(SpanProcessor):
    """
    Sends finished spans over OTLP/HTTP protobuf, in place of `BatchSpanProcessor` with `OTLPSpanExporter`.

    `on_end` copies the span into Rust and returns; a native thread, started with the first span, batches,
    encodes and sends them without taking the GIL. Only sampled spans are exported, as with `BatchSpanProcessor`.

    `endpoint` is the URL spans are posted to, `/v1/traces` included, as for `OTLPSpanExporter`.
    """

    def __init__(self, *, endpoint: str | None = None) -> None:
        self._native = _fastotel.Processor(endpoint or DEFAULT_ENDPOINT)

    def on_end(self, span: ReadableSpan) -> None:
        self._native.on_end(span)

    def shutdown(self) -> None:
        self._native.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        # A negative timeout is an expired one, not an OverflowError from the native side
        return self._native.force_flush(max(timeout_millis, 0))
