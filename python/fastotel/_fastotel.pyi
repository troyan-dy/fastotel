# The native module built from src/lib.rs

from opentelemetry.sdk.trace import ReadableSpan

class Processor:
    """
    The native half of `OTLPSpanProcessor`: copies spans and hands them to the export pipeline.
    """

    def __new__(
        cls,
        endpoint: str,
        max_queue_size: int,
        schedule_delay_millis: float,
        max_export_batch_size: int,
        export_timeout_millis: float,
    ) -> Processor: ...
    def on_end(self, span: ReadableSpan) -> None: ...
    def force_flush(self, timeout_millis: int) -> bool: ...
    def shutdown(self) -> bool: ...
    def dropped_spans(self) -> int: ...
