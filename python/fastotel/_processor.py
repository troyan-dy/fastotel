import atexit
import logging
import os
import sys
import threading
import time
import weakref
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from urllib.parse import urlsplit

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

    `endpoint`, `headers`, `timeout` (seconds, for the export of a batch, retries included), `compression` and the
    TLS files are the arguments of `OTLPSpanExporter`, read with its `OTEL_EXPORTER_OTLP_*` variables, precedence
    and defaults; `endpoint` is the URL spans are posted to, `/v1/traces` included. A batch that fails with a
    retryable status or a connection error is sent again with exponential backoff, honouring `Retry-After`, until
    `timeout` has passed. An `https://` endpoint is verified against the OS trust store, or against
    `certificate_file` when it is given.
    The other arguments are those of `BatchSpanProcessor`, with its `OTEL_BSP_*` variables, defaults and checks: a
    batch leaves at `max_export_batch_size` spans or `schedule_delay_millis` after the previous export, a span ended
    while `max_queue_size` spans wait is dropped, and `shutdown` waits `export_timeout_millis` for the last export.

    `TracerProvider` shuts its processors down at exit; without that, spans still queued at exit are lost. A panic
    in the native code is caught, counted and logged on the `fastotel` logger; it never reaches the application.
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
        # As by requests, the TLS files matter only to an https:// endpoint; a file that cannot be read or holds no
        # certificate or key raises here, where the reference fails every export
        https = urlsplit(self._endpoint).scheme == "https"
        client_certificate = _read(self._client_certificate_file) if https else None
        self._native = _fastotel.Processor(
            self._endpoint,
            list(self._headers.items()),
            self._timeout,
            self._compression,
            _read(self._certificate_file) if https else None,
            client_certificate,
            # A key without a certificate is not used, and a certificate without a key file holds the key
            _read(self._client_key_file) if client_certificate is not None else None,
            max_queue_size,
            schedule_delay_millis,
            max_export_batch_size,
            export_timeout_millis,
        )
        _processors.add(self)

    def on_end(self, span: ReadableSpan) -> None:
        self._native.on_end(span)

    def shutdown(self) -> None:
        """
        Exports what is queued, waiting up to `export_timeout_millis`, and stops the worker. Spans ended afterwards
        are dropped and counted; a second call does nothing.
        """
        self._native.shutdown()

    def force_flush(self, timeout_millis: float | None = 30000) -> bool:
        """
        Exports every span ended before the call. False when that takes longer than `timeout_millis`, and after
        `shutdown`, as `BatchSpanProcessor` returns; None waits `export_timeout_millis`, as `BatchSpanProcessor` did
        while it still honoured the timeout.
        """
        if timeout_millis is None:
            timeout_millis = self._export_timeout_millis
        # Negative or NaN is an expired timeout, too many milliseconds to hold no limit
        return self._native.force_flush(timeout_millis)


# Every processor alive, for the exit handler
_processors: "weakref.WeakSet[OTLPSpanProcessor]" = weakref.WeakSet()
# The frames of the calls into the native module: a thread with one of them on its stack is inside it
_NATIVE_CALLS = frozenset(
    {OTLPSpanProcessor.on_end.__code__, OTLPSpanProcessor.force_flush.__code__, OTLPSpanProcessor.shutdown.__code__}
)
# How long the exit handler waits for other threads to leave the native module
_EXIT_WAIT_SECONDS = 1.0


@atexit.register
def _at_exit() -> None:
    """
    Keeps daemon threads out of the native module before the interpreter finalizes.

    Registered at import, so it runs after the exit handlers of the providers created later, which shut their
    processors down. From here on `on_end` drops and counts spans without calling into Python, and `force_flush`
    and `shutdown` waiting on other threads return False. Before 3.14 CPython ends a daemon thread that takes the
    GIL during finalization with `pthread_exit`, which on glibc unwinds its stack and aborts the process when Rust
    frames are on it, so this also waits until no other thread is inside a call to the native module.
    """
    for processor in list(_processors):
        processor._native.exiting()
    if sys.version_info >= (3, 14):
        # Such a thread hangs instead, which is harmless
        return
    deadline = time.monotonic() + _EXIT_WAIT_SECONDS
    while _inside_native_calls() and time.monotonic() < deadline:
        # Lets those threads take the GIL and finish the call they are in
        time.sleep(0.001)


def _inside_native_calls() -> bool:
    current = threading.get_ident()
    for ident, frame in sys._current_frames().items():
        if ident == current:
            continue
        while frame is not None:
            if frame.f_code in _NATIVE_CALLS:
                return True
            frame = frame.f_back  # type: ignore[assignment]
    return False


def _read(path: str | None) -> bytes | None:
    return Path(path).read_bytes() if path else None


def _int_from_environ(name: str, default: int) -> int:
    # As BatchSpanProcessor reads its variables: int() of the value, or the default and the same error log
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.exception("Unable to parse value for %s as integer. Defaulting to %s.", name, default)
        return default
