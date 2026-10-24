import atexit
import sys
import threading
import time
import weakref
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from types import FrameType
from urllib.parse import urlsplit

from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor

from fastotel import _batch, _exporter, _fastotel


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

    def __init__(  # noqa: WPS211 - the arguments of OTLPSpanExporter and BatchSpanProcessor
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

        self._max_queue_size = _batch.max_queue_size(max_queue_size)
        self._schedule_delay_millis = _batch.schedule_delay_millis(schedule_delay_millis)
        self._max_export_batch_size = _batch.max_export_batch_size(max_export_batch_size)
        self._export_timeout_millis = _batch.export_timeout_millis(export_timeout_millis)
        _batch.check(self._max_queue_size, self._schedule_delay_millis, self._max_export_batch_size)
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
            None if client_certificate is None else _read(self._client_key_file),
            self._max_queue_size,
            self._schedule_delay_millis,
            self._max_export_batch_size,
            self._export_timeout_millis,
        )
        with _exit_lock:
            _processors.add(self)
            if _exited.is_set():
                # Created by an exit handler that runs after fastotel's
                self._native.exiting(_hang_at_exit())

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


# Every processor alive, for the exit handler, and whether it has run
_processors: "weakref.WeakSet[OTLPSpanProcessor]" = weakref.WeakSet()
_exited = threading.Event()
_exit_lock = threading.Lock()
# The frames of the calls into the native module
_NATIVE_CALLS = frozenset(
    (OTLPSpanProcessor.on_end.__code__, OTLPSpanProcessor.force_flush.__code__, OTLPSpanProcessor.shutdown.__code__)
)
# How long the exit handler waits for threads running Python code the native module called, and for threads that
# only have to take the GIL back to leave it
_EXIT_WAIT_SECONDS = 2.0
_EXIT_WAIT_DETACHED_SECONDS = 10.0
# How often it looks again, letting those threads take the GIL and leave in between
_EXIT_POLL_SECONDS = 0.001


def _hang_at_exit() -> bool:
    # Before 3.14 a thread back from a wait hangs rather than take the GIL back, as 3.14 hangs daemon threads; a
    # build without the GIL (3.13t, built from source) gets no protection from it
    is_gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)
    return sys.version_info < (3, 14) and is_gil_enabled()


@atexit.register
def _at_exit() -> None:
    """
    Keeps daemon threads out of the native module before the interpreter finalizes.

    Registered at import, so it runs after the exit handlers of the providers created later, which shut their
    processors down. From here on, on other threads, `on_end` drops and counts spans without calling into Python,
    and `force_flush` and `shutdown` return False without letting go of the GIL, ending the waits they are in. Before
    3.14 CPython ends a daemon thread that takes the GIL during finalization with `pthread_exit`, which on glibc
    unwinds its stack and aborts the process when Rust frames are on it, so a thread back from such a wait hangs
    instead, and this waits until no other thread can take the GIL inside the native module.
    """
    hang = _hang_at_exit()
    with _exit_lock:
        _exited.set()
        processors = list(_processors)
    for processor in processors:
        processor._native.exiting(hang)
    if hang:
        _wait_for_native_code()


def _wait_for_native_code() -> None:
    started = time.monotonic()
    inside = _inside_native_code()
    while inside is not None:
        waited = time.monotonic() - started
        if waited > _EXIT_WAIT_DETACHED_SECONDS or (inside == "python" and waited > _EXIT_WAIT_SECONDS):
            break
        time.sleep(_EXIT_POLL_SECONDS)
        inside = _inside_native_code()


def _inside_native_code() -> str | None:
    """
    How another thread can take the GIL inside Rust code, if one can: "detached" when it is back from a wait in
    `force_flush` or `shutdown` without the GIL and does not hang, "python" when it runs Python code that the native
    module called (a span's properties, a log), which shows as a frame above one of the calls into it. Otherwise
    native code takes no GIL, so a thread whose topmost frame is the call itself is about to enter it, or back from
    it, and harmless.
    """
    if _fastotel.detached():
        return "detached"
    current = threading.get_ident()
    for ident, top in sys._current_frames().items():
        if ident != current and _called_from_native_code(top):
            return "python"
    return None


def _called_from_native_code(top: FrameType) -> bool:
    # Whether a call into the native module is on the stack below the topmost frame
    frame: FrameType | None = top
    while frame is not None:
        if frame.f_code in _NATIVE_CALLS:
            return frame is not top
        frame = frame.f_back
    return False


def _read(path: str | None) -> bytes | None:
    return Path(path).read_bytes() if path else None
