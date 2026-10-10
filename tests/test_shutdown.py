"""
force_flush and shutdown of #12 with the semantics of BatchSpanProcessor, and Rust panics caught at the boundary.
"""

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from fastotel import OTLPSpanProcessor
from fastotel._fastotel import PanicException
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.sampling import Decision, StaticSampler
from receiver import FakeReceiver


def _span(name: str = "span", provider: TracerProvider | None = None) -> ReadableSpan:
    span = (provider or TracerProvider()).get_tracer("app").start_span(name)
    span.end()
    assert isinstance(span, ReadableSpan)
    return span


def test_force_flush_returns_false_when_the_export_outlasts_the_timeout(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    with receiver.stalled():
        processor.on_end(_span())
        started = time.monotonic()
        assert processor.force_flush(200) is False
        waited = time.monotonic() - started
    # Timers fire a little early on Windows, and a busy runner wakes late
    assert 0.15 < waited < 5
    # The export goes on and the next flush waits for it
    assert processor.force_flush()
    assert len(receiver.spans()) == 1
    processor.shutdown()


def test_force_flush_exports_what_ended_before_it(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, schedule_delay_millis=60000)
    for i in range(3):
        processor.on_end(_span(f"span {i}"))
    assert processor.force_flush()
    assert [span.name for span in receiver.spans()] == ["span 0", "span 1", "span 2"]
    # Nothing queued: no request
    assert processor.force_flush()
    assert len(receiver.received) == 1
    processor.shutdown()


@pytest.mark.parametrize("timeout_millis", [None, 10000, 2500.5, 0, -1, math.nan, math.inf])
def test_force_flush_takes_any_timeout_batch_span_processor_takes(
    receiver: FakeReceiver, timeout_millis: float | None
) -> None:
    # None waits export_timeout_millis, as BatchSpanProcessor did while it honoured the timeout; no time left
    # may return before the export without raising
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.on_end(_span())
    flushed = processor.force_flush(timeout_millis)
    if timeout_millis is None or timeout_millis > 0:
        assert flushed
        assert len(receiver.spans()) == 1
    processor.shutdown()
    assert len(receiver.spans()) == 1


def test_force_flush_none_waits_export_timeout_millis(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, export_timeout_millis=300)
    with receiver.stalled():
        processor.on_end(_span())
        started = time.monotonic()
        assert processor.force_flush(None) is False
        waited = time.monotonic() - started
    assert 0.25 < waited < 5
    processor.shutdown()


class _Recording:
    """
    A span that records which of its attributes are read.
    """

    def __init__(self, span: ReadableSpan) -> None:
        self.span = span
        self.read: list[str] = []

    def __getattr__(self, name: str) -> Any:
        self.read.append(name)
        return getattr(self.span, name)


def test_on_end_after_shutdown_reads_nothing_from_the_span(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.shutdown()

    record_only = TracerProvider(sampler=StaticSampler(Decision.RECORD_ONLY))
    sampled, not_sampled = _Recording(_span()), _Recording(_span(provider=record_only))
    processor.on_end(sampled)  # type: ignore[arg-type]
    processor.on_end(not_sampled)  # type: ignore[arg-type]

    # Not even whether it is sampled, which would run Python code inside the native call: each is counted as
    # dropped, where BatchSpanProcessor counts only the sampled one
    assert sampled.read == not_sampled.read == []
    assert processor._native.dropped_spans() == 2
    assert receiver.received == []


def test_shutdown_from_many_threads_at_once_exports_once(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, schedule_delay_millis=60000)
    for i in range(10):
        processor.on_end(_span(f"span {i}"))
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda _: processor.shutdown(), range(8)))
    assert len(receiver.spans()) == 10
    assert processor._native.dropped_spans() == 0


def test_shutdown_before_any_span_drops_and_counts_later_spans(receiver: FakeReceiver) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    processor.shutdown()
    processor.on_end(_span())
    assert processor.force_flush() is False
    assert receiver.received == []
    assert processor._native.dropped_spans() == 1


class _Panicking:
    """
    A span whose copy panics in Rust: PyO3 resumes a panic when the Python code it calls raises PanicException.
    """

    @property
    def context(self) -> Any:
        raise PanicException("a bug in the copy")


def test_a_panic_in_on_end_is_caught_logged_and_counted(
    receiver: FakeReceiver, caplog: pytest.LogCaptureFixture
) -> None:
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint)
    with caplog.at_level(logging.ERROR, logger="fastotel"):
        processor.on_end(_Panicking())  # type: ignore[arg-type]

    assert [(record.name, record.levelno, record.getMessage()) for record in caplog.records] == [
        ("fastotel", logging.ERROR, "fastotel panicked in on_end: a bug in the copy")
    ]
    assert processor._native.panics() == 1
    # The processor goes on
    processor.on_end(_span())
    processor.shutdown()
    assert len(receiver.spans()) == 1


def test_exiting_ends_the_waits_of_other_threads_and_drops_their_spans(receiver: FakeReceiver) -> None:
    # What the exit handler does to each processor before the interpreter finalizes
    processor = OTLPSpanProcessor(endpoint=receiver.endpoint, export_timeout_millis=60000)
    with ThreadPoolExecutor(1) as other:
        with receiver.stalled():
            processor.on_end(_span("before"))
            flushed = other.submit(processor.force_flush, 60000)
            assert len(receiver.wait_for_requests(1)) == 1
            started = time.monotonic()
            processor._native.exiting()
            # A daemon thread waiting would otherwise take the GIL back during finalization
            assert flushed.result(timeout=10) is False
            assert time.monotonic() - started < 5
        # Other threads: spans dropped and counted, no flush, no shutdown
        other.submit(processor.on_end, _span("other")).result()
        assert other.submit(processor.force_flush).result() is False
        other.submit(processor.shutdown).result()
        assert processor._native.dropped_spans() == 1
        # The thread that ran the exit handler goes on, as the exit handlers after it, a provider's among them, do
        processor.on_end(_span("exiting"))
        assert processor.force_flush()
        processor.shutdown()
        other.submit(processor.on_end, _span("other")).result()
    assert [span.name for span in receiver.spans()] == ["before", "exiting"]
    assert processor._native.dropped_spans() == 2
