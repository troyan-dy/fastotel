import logging
import os
from collections.abc import Mapping
from enum import Enum

from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor

from fastotel import _exporter, _fastotel

# The variables and defaults of BatchSpanProcessor
OTEL_BSP_MAX_QUEUE_SIZE = "OTEL_BSP_MAX_QUEUE_SIZE"
OTEL_BSP_SCHEDULE_DELAY = "OTEL_BSP_SCHEDULE_DELAY"
OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
OTEL_BSP_EXPORT_TIMEOUT = "OTEL_BSP_EXPORT_TIMEOUT"
_DEFAULT_MAX_QUEUE_SIZE = 2048
_DEFAULT_SCHEDULE_DELAY_MILLIS = 5000
_DEFAULT_MAX_EXPORT_BATCH_SIZE = 512
_DEFAULT_EXPORT_TIMEOUT_MILLIS = 30000

logger = logging.getLogger("fastotel")


class OTLPSpanProcessor(SpanProcessor):
    """
    Sends finished spans over OTLP/HTTP protobuf, in place of `BatchSpanProcessor` with `OTLPSpanExporter`.

    `on_end` copies the span into Rust and returns; a native thread, started with the first span, batches,
    encodes and sends them without taking the GIL. Only sampled spans are exported, as with `BatchSpanProcessor`.

    `endpoint`, `headers`, `timeout` (seconds, for each request), `compression` and the TLS files are the arguments
    of `OTLPSpanExporter`, read with its `OTEL_EXPORTER_OTLP_*` variables, precedence and defaults; `endpoint` is
    the URL spans are posted to, `/v1/traces` included. Compression and the TLS files are read but not applied yet.
    The other arguments are those of `BatchSpanProcessor`, with its `OTEL_BSP_*` variables, defaults and checks: a
    batch leaves at `max_export_batch_size` spans or `schedule_delay_millis` after the previous export, a span ended
    while `max_queue_size` spans wait is dropped, and `shutdown` waits `export_timeout_millis` for the last export.
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        compression: str | Enum | None = None,
        certificate_file: str | None = None,
        client_key_file: str | None = None,
        client_certificate_file: str | None = None,
        max_queue_size: int | None = None,
        schedule_delay_millis: float | None = None,
        max_export_batch_size: int | None = None,
        export_timeout_millis: float | None = None,
    ) -> None:
        # In the order the reference reads them, so that the warnings come in its order too
        self._endpoint = _exporter.endpoint(endpoint)
        self._compression = _exporter.compression(compression)
        self._certificate_file = _exporter.tls_file(
            certificate_file, _exporter.OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE, _exporter.OTEL_EXPORTER_OTLP_CERTIFICATE
        )
        self._client_key_file = _exporter.tls_file(
            client_key_file, _exporter.OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY, _exporter.OTEL_EXPORTER_OTLP_CLIENT_KEY
        )
        self._client_certificate_file = _exporter.tls_file(
            client_certificate_file,
            _exporter.OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE,
            _exporter.OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE,
        )
        self._timeout = _exporter.timeout(timeout)
        self._headers = _exporter.headers(headers)

        if max_queue_size is None:
            max_queue_size = _int_from_environ(OTEL_BSP_MAX_QUEUE_SIZE, _DEFAULT_MAX_QUEUE_SIZE)
        if schedule_delay_millis is None:
            schedule_delay_millis = _int_from_environ(OTEL_BSP_SCHEDULE_DELAY, _DEFAULT_SCHEDULE_DELAY_MILLIS)
        if max_export_batch_size is None:
            max_export_batch_size = _int_from_environ(OTEL_BSP_MAX_EXPORT_BATCH_SIZE, _DEFAULT_MAX_EXPORT_BATCH_SIZE)
        if export_timeout_millis is None:
            export_timeout_millis = _int_from_environ(OTEL_BSP_EXPORT_TIMEOUT, _DEFAULT_EXPORT_TIMEOUT_MILLIS)
        # The checks and messages of BatchSpanProcessor
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")
        # `not > 0` also refuses NaN, which the SDK lets through to spin its worker
        if not schedule_delay_millis > 0:
            raise ValueError("schedule_delay_millis must be positive.")
        if max_export_batch_size <= 0:
            raise ValueError("max_export_batch_size must be a positive integer.")
        if max_export_batch_size > max_queue_size:
            raise ValueError("max_export_batch_size must be less than or equal to max_queue_size.")

        self._max_queue_size = max_queue_size
        self._schedule_delay_millis = schedule_delay_millis
        self._max_export_batch_size = max_export_batch_size
        self._export_timeout_millis = export_timeout_millis
        self._native = _fastotel.Processor(
            self._endpoint,
            list(self._headers.items()),
            self._timeout,
            max_queue_size,
            schedule_delay_millis,
            max_export_batch_size,
            export_timeout_millis,
        )

    def on_end(self, span: ReadableSpan) -> None:
        self._native.on_end(span)

    def shutdown(self) -> None:
        self._native.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        # A negative timeout is an expired one, not an OverflowError from the native side
        return self._native.force_flush(max(timeout_millis, 0))


def _int_from_environ(name: str, default: int) -> int:
    # As BatchSpanProcessor reads its variables: int() of the value, or the default and the same error log
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.exception("Unable to parse value for %s as integer. Defaulting to %s.", name, default)
        return default
