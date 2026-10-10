# The native module built from src/lib.rs

from opentelemetry.sdk.trace import ReadableSpan

class Processor:
    """
    The native half of `OTLPSpanProcessor`: copies spans and hands them to the export pipeline.
    """

    def __new__(
        cls,
        endpoint: str,
        headers: list[tuple[str, str]],
        timeout: float,
        compression: str,
        certificate: bytes | None,
        client_certificate: bytes | None,
        client_key: bytes | None,
        max_queue_size: int,
        schedule_delay_millis: float,
        max_export_batch_size: int,
        export_timeout_millis: float,
    ) -> Processor: ...
    def on_end(self, span: ReadableSpan) -> None: ...
    def force_flush(self, timeout_millis: float) -> bool: ...
    def shutdown(self) -> bool: ...
    def dropped_spans(self) -> int: ...
    def failed_spans(self) -> int: ...
    def rejected_spans(self) -> int: ...
    def retries(self) -> int: ...
    def panics(self) -> int: ...

class PanicException(BaseException):
    """
    What a Rust panic becomes in Python; raised from Python code that the native side calls, it panics there.
    """
