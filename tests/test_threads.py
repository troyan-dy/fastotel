"""
Many threads ending spans alongside force_flush and shutdown (#12): on a free-threaded build they run in
parallel, on a GIL build they switch at any point. Every span ends up exported exactly once or counted as dropped.
"""

import threading
import time
from collections.abc import Callable

from fastotel import OTLPSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
from receiver import FakeReceiver

# The ticket asks for at least 32
EMITTING = 32
FLUSHING = 4
SPANS = 1000
# A deadlock fails the test after this many seconds rather than hang the run; a slow runner needs a few
TIMEOUT = 120


def _run(  # noqa: WPS231 - the threads of the test, defined where they share its state
    receiver: FakeReceiver, midway: Callable[[OTLPSpanProcessor], None]
) -> tuple[OTLPSpanProcessor, int]:
    """
    Ends SPANS spans on each of EMITTING threads while FLUSHING threads flush in a loop, calls `midway` once
    half the spans have ended, then shuts down; the processor and the spans ended.
    """
    # Quick exports, so that batches leave by size, by delay and by flush; spans are dropped when the queue fills
    processor = OTLPSpanProcessor(
        endpoint=receiver.endpoint, max_queue_size=8192, max_export_batch_size=512, schedule_delay_millis=20
    )
    provider = TracerProvider()
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("app")
    ended = [0 for _ in range(EMITTING)]
    half = threading.Semaphore(0)
    start = threading.Barrier(EMITTING + FLUSHING + 1)
    emitting = threading.Event()
    emitting.set()
    errors: list[BaseException] = []

    def emit(index: int) -> None:
        try:  # noqa: WPS229 - whatever the thread raises fails the test
            start.wait(TIMEOUT)
            for count in range(SPANS):
                tracer.start_span("span").end()
                ended[index] += 1
                if count == SPANS // 2:
                    half.release()
        except BaseException as error:
            errors.append(error)
            half.release()

    def flush() -> None:
        try:  # noqa: WPS229 - whatever the thread raises fails the test
            start.wait(TIMEOUT)
            while emitting.is_set():
                processor.force_flush(10000)
        except BaseException as error:
            errors.append(error)

    # Daemon threads, so that a deadlock fails the test instead of keeping the process alive
    emitters = [threading.Thread(target=emit, args=(index,), daemon=True) for index in range(EMITTING)]
    flushers = [threading.Thread(target=flush, daemon=True) for _ in range(FLUSHING)]
    for thread in emitters + flushers:
        thread.start()
    start.wait(TIMEOUT)
    for _ in range(EMITTING):
        assert half.acquire(timeout=TIMEOUT), "an emitting thread is stuck"
    midway(processor)
    deadline = time.monotonic() + TIMEOUT
    for thread in emitters:
        thread.join(max(deadline - time.monotonic(), 0))
    emitting.clear()
    for thread in flushers:
        thread.join(max(deadline - time.monotonic(), 0))
    assert not any(thread.is_alive() for thread in emitters + flushers), "a thread is stuck"
    assert errors == []
    # Every export has been answered, and so recorded, once shutdown returns True; after one midway, at once
    assert processor._native.shutdown()
    return processor, sum(ended)


def _check(receiver: FakeReceiver, processor: OTLPSpanProcessor, ended: int) -> int:
    native = processor._native
    exported = [span.span_id for span in receiver.spans()]
    assert len(exported) == len(set(exported)), "a span was exported twice"
    assert (native.failed_spans(), native.panics()) == (0, 0)
    assert len(exported) + native.dropped_spans() == ended
    return len(exported)


def test_on_end_force_flush_and_a_shutdown_midway_on_many_threads(receiver: FakeReceiver) -> None:
    def shut_down(processor: OTLPSpanProcessor) -> None:
        assert processor._native.shutdown()

    processor, ended = _run(receiver, shut_down)
    assert ended == EMITTING * SPANS
    # How many end after the shutdown depends on the scheduler; those are dropped and counted
    assert _check(receiver, processor, ended) > 0


def test_on_end_and_force_flush_on_many_threads(receiver: FakeReceiver) -> None:
    processor, ended = _run(receiver, lambda processor: None)
    assert ended == EMITTING * SPANS
    _check(receiver, processor, ended)
