"""
Batching and the queue of #9: OTEL_BSP_* as BatchSpanProcessor reads them, batches by size and by delay, and a
full queue that drops spans without ever blocking on_end.
"""

import logging
import math
import socket
import subprocess
import sys
import textwrap
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from fastotel import OTLPSpanProcessor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from receiver import FakeReceiver

VARIABLES = (
    "OTEL_BSP_MAX_QUEUE_SIZE",
    "OTEL_BSP_SCHEDULE_DELAY",
    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
    "OTEL_BSP_EXPORT_TIMEOUT",
)
SETTINGS = ("max_queue_size", "schedule_delay_millis", "max_export_batch_size", "export_timeout_millis")


class _NoExport(SpanExporter):
    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


# The effective settings, or the error at construction, and what was logged
Outcome = tuple[tuple[Any, ...] | str, list[tuple[int, str, bool]]]


def _logged(caplog: pytest.LogCaptureFixture, logger: str) -> list[tuple[int, str, bool]]:
    return [
        (record.levelno, record.getMessage(), record.exc_info is not None)
        for record in caplog.records
        if record.name == logger
    ]


def _reference(caplog: pytest.LogCaptureFixture, kwargs: dict[str, Any]) -> Outcome:
    caplog.clear()
    try:
        processor = BatchSpanProcessor(_NoExport(), **kwargs)
    except ValueError as error:
        return str(error), _logged(caplog, "opentelemetry.sdk.trace.export")
    # Current SDKs keep the settings in a BatchProcessor, older ones on the processor itself
    holder = getattr(processor, "_batch_processor", processor)
    settings = tuple(getattr(holder, f"_{name}" if hasattr(holder, f"_{name}") else name) for name in SETTINGS)
    processor.shutdown()  # type: ignore[no-untyped-call]
    return settings, _logged(caplog, "opentelemetry.sdk.trace.export")


def _fastotel(caplog: pytest.LogCaptureFixture, kwargs: dict[str, Any]) -> Outcome:
    caplog.clear()
    try:
        processor = OTLPSpanProcessor(**kwargs)
    except ValueError as error:
        return str(error), _logged(caplog, "fastotel")
    settings = tuple(getattr(processor, f"_{name}") for name in SETTINGS)
    processor.shutdown()
    return settings, _logged(caplog, "fastotel")


@pytest.mark.parametrize(
    ("environ", "kwargs"),
    [
        ({}, {}),
        # Each variable, valid
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "4096"}, {}),
        ({"OTEL_BSP_SCHEDULE_DELAY": "250"}, {}),
        ({"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "100"}, {}),
        ({"OTEL_BSP_EXPORT_TIMEOUT": "1000"}, {}),
        # What int() takes
        ({"OTEL_BSP_MAX_QUEUE_SIZE": " 1_000 ", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "+10"}, {}),
        # Not integers: the default and an error log
        *[({name: value}, {}) for name in VARIABLES for value in ("", "abc", "1.5", "0x10")],
        # Out of range
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "0"}, {}),
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "-1"}, {}),
        ({"OTEL_BSP_SCHEDULE_DELAY": "0"}, {}),
        ({"OTEL_BSP_SCHEDULE_DELAY": "-5"}, {}),
        ({"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "0"}, {}),
        ({"OTEL_BSP_EXPORT_TIMEOUT": "0"}, {}),
        ({"OTEL_BSP_EXPORT_TIMEOUT": "-1"}, {}),
        # A batch larger than the queue
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "100"}, {}),
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "100", "OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "100"}, {}),
        ({"OTEL_BSP_MAX_EXPORT_BATCH_SIZE": "4096"}, {}),
        # Arguments override the variables, which are then not read at all
        (
            dict.fromkeys(VARIABLES, "7"),
            {"max_queue_size": 64, "schedule_delay_millis": 10, "max_export_batch_size": 8, "export_timeout_millis": 9},
        ),
        (dict.fromkeys(VARIABLES, "abc"), {"max_queue_size": 64, "schedule_delay_millis": 0.5}),
        ({}, {"max_export_batch_size": 4096}),
        ({"OTEL_BSP_MAX_QUEUE_SIZE": "8"}, {"max_export_batch_size": 16}),
        ({}, {"max_queue_size": 0}),
        ({}, {"schedule_delay_millis": 0}),
        ({}, {"schedule_delay_millis": -1.5}),
        ({}, {"max_export_batch_size": -1}),
        ({}, {"export_timeout_millis": -1.5}),
    ],
)
def test_reads_the_settings_as_batch_span_processor_does(
    environ: dict[str, str], kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    caplog.set_level(logging.DEBUG)

    reference = _reference(caplog, kwargs)
    if isinstance(reference[0], str) and reference[0].startswith("invalid literal"):
        pytest.skip("this SDK fails on a variable it cannot parse; fastotel does what current ones do")
    assert _fastotel(caplog, kwargs) == reference


def test_refuses_a_schedule_delay_of_nan() -> None:
    # A deliberate difference (ADR 0004): BatchSpanProcessor takes it and its worker spins
    with pytest.raises(ValueError, match="schedule_delay_millis must be positive"):
        OTLPSpanProcessor(schedule_delay_millis=math.nan)


def _spans(count: int) -> list[ReadableSpan]:
    tracer = TracerProvider().get_tracer("app")
    spans: list[ReadableSpan] = []
    for _ in range(count):
        span = tracer.start_span("span")
        span.end()
        assert isinstance(span, ReadableSpan)
        spans.append(span)
    return spans


def _sizes(receiver: FakeReceiver) -> list[int]:
    return [
        sum(
            len(scope_spans.spans)
            for resource_spans in request.resource_spans
            for scope_spans in resource_spans.scope_spans
        )
        for request in receiver.requests
    ]


def test_a_full_batch_leaves_without_waiting_for_the_delay(
    receiver: FakeReceiver, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "5")
    monkeypatch.setenv("OTEL_BSP_SCHEDULE_DELAY", "60000")
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    for span in _spans(12):
        processor.on_end(span)

    receiver.wait_for_requests(2)
    time.sleep(0.2)
    # The last two spans wait for the delay, or for a flush
    assert _sizes(receiver) == [5, 5]
    assert processor.force_flush()
    assert _sizes(receiver) == [5, 5, 2]
    processor.shutdown()


def test_a_partial_batch_leaves_when_the_delay_expires(receiver: FakeReceiver, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_BSP_SCHEDULE_DELAY", "300")
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    spans = _spans(3)

    started = time.monotonic()
    for span in spans:
        processor.on_end(span)
    assert receiver.requests == []
    receiver.wait_for_requests(1)
    waited = time.monotonic() - started

    assert _sizes(receiver) == [3]
    # The delay runs from the start of the worker, which the first span starts
    assert 0.25 < waited < 5
    processor.shutdown()


def test_a_full_queue_drops_and_counts_spans_without_blocking_on_end(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(
        endpoint=receiver.endpoint, max_queue_size=100, max_export_batch_size=10, schedule_delay_millis=60000
    )
    first, queued, overflow = _spans(10), _spans(100), _spans(50)
    with receiver.stalled():
        # A full batch leaves and hangs on the receiver: from then on nothing leaves the queue
        for span in first:
            processor.on_end(span)
        assert len(receiver.wait_for_requests(1)) == 1

        took = []
        for span in queued + overflow:
            started = time.perf_counter()
            processor.on_end(span)
            took.append(time.perf_counter() - started)
        # As BatchSpanProcessor, the batch being sent is out of the queue
        assert processor._native.dropped_spans() == len(overflow)
    took.sort()
    # Microseconds each, against the 30 s export timeout; generous for a busy CI runner
    assert took[len(took) // 2] < 0.001
    assert took[-1] < 0.1

    assert processor.force_flush()
    assert sum(_sizes(receiver)) == len(first) + len(queued)
    processor.shutdown()


def test_shutdown_waits_for_the_export_timeout_at_most(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, export_timeout_millis=300)
    with receiver.stalled():
        processor.on_end(_spans(1)[0])
        started = time.monotonic()
        processor.shutdown()
        waited = time.monotonic() - started
    assert 0.25 < waited < 5


def _closed_port_endpoint() -> str:
    with socket.socket() as unused:
        unused.bind(("127.0.0.1", 0))
        port = unused.getsockname()[1]
    return f"http://127.0.0.1:{port}/v1/traces"


# Run in a process of its own, so that its peak memory is that of the spans it pushes
_PUSH_MANY = """
import resource, sys, time
sys.path.insert(0, {tests!r})
from receiver import FakeReceiver
from fastotel import OTLPSpanProcessor
from opentelemetry.sdk.trace import TracerProvider

attributes = {{f"key {{i}}": "v" * 50 for i in range(10)}}
span = TracerProvider().get_tracer("app").start_span("x" * 200, attributes=attributes)
span.end()
with FakeReceiver() as receiver, receiver.stalled():
    endpoint = {endpoint!r} or receiver.endpoint
    processor = OTLPSpanProcessor(endpoint=endpoint)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    for _ in range({count}):
        processor.on_end(span)
    grown = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before
    # Kilobytes on Linux, bytes on macOS
    print(grown * (1 if sys.platform == "darwin" else 1024), processor._native.dropped_spans(), flush=True)
"""


@pytest.mark.skipif(sys.platform == "win32", reason="measures memory through the resource module")
@pytest.mark.parametrize("down", ["stalled", "refusing"])
def test_memory_stays_bounded_while_the_receiver_is_down(down: str) -> None:
    count = 50_000
    endpoint = _closed_port_endpoint() if down == "refusing" else ""
    script = textwrap.dedent(_PUSH_MANY).format(tests=str(Path(__file__).parent), endpoint=endpoint, count=count)
    result = subprocess.run(  # noqa: S603 - the test's own interpreter and script
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120, check=True
    )
    grown, dropped = map(int, result.stdout.split())

    # Each span holds about 2 KB: kept, they would take 100 MB; the queue holds 2048 of them and a batch of 512
    assert grown < 32 * 2**20
    if down == "stalled":
        assert dropped == count - 2048 - 512
