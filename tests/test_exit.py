"""
Interpreter exit (#12), each case in a process of its own with a time limit: the provider's shutdown at exit
exports what is queued within the export timeout, and without it the process still exits at once and cleanly.
"""

import subprocess
import sys
import textwrap
import time

import pytest
from receiver import FakeReceiver

# Well above what a slow CI runner takes to start Python and import the SDK, well below the waits a hang would cost
LIMIT = 60

_CHECK = """
import atexit, sys, threading, time

# Appended to by a script's own exit handler, which runs before the provider's
exit_started = []

@atexit.register
def _check_daemon_threads_are_out():
    # Registered before fastotel's exit handler, so it runs after it: no thread may be left where finalization
    # would end it inside Rust code; the frames tell where
    from fastotel import _processor
    if sys.version_info < (3, 14) and _processor._inside_native_code() is not None:
        frames = sys._current_frames()
        import traceback
        for ident, frame in frames.items():
            print("THREAD", ident, "".join(traceback.format_stack(frame)), file=sys.stderr)
        print("STILL INSIDE fastotel after its exit handler", file=sys.stderr, flush=True)
    if exit_started:
        print(f"EXIT WAITED {time.monotonic() - exit_started[0]}", file=sys.stderr, flush=True)

from opentelemetry.sdk.trace import TracerProvider
"""
_IMPORT = """
from fastotel import OTLPSpanProcessor

endpoint = sys.argv[1]
"""


def _start(script: str, endpoint: str, before_import: str = "") -> subprocess.Popen[str]:
    """
    Runs `script` after the prelude; `before_import` runs before fastotel is imported.
    """
    source = _CHECK + textwrap.dedent(before_import) + _IMPORT + textwrap.dedent(script)
    return subprocess.Popen(  # noqa: S603 - the test's own interpreter and script
        [sys.executable, "-X", "faulthandler", "-c", source, endpoint],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _exit(process: subprocess.Popen[str], stdin: str = "") -> tuple[float, str]:
    """
    Waits for the process to exit, failing the test after LIMIT seconds; how long it took and its stderr.
    """
    started = time.monotonic()
    try:
        _, stderr = process.communicate(stdin, timeout=LIMIT)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        pytest.fail(f"the process did not exit within {LIMIT} s")
    took = time.monotonic() - started
    assert process.returncode == 0, stderr
    # Neither a panic nor a crash on the way out, nor an exception ignored by the interpreter
    for sign in ("panicked", "Fatal Python error", "Traceback", "Exception ignored", "STILL INSIDE"):
        assert sign not in stderr, stderr
    return took, stderr


def _run(script: str, endpoint: str, before_import: str = "") -> float:
    return _exit(_start(script, endpoint, before_import))[0]


def test_the_providers_shutdown_at_exit_exports_what_is_queued(receiver: FakeReceiver) -> None:
    _run(
        """
        provider = TracerProvider()
        # Only shutdown exports these: the delay is not over before exit
        provider.add_span_processor(OTLPSpanProcessor(endpoint=endpoint, schedule_delay_millis=600_000))
        for i in range(3):
            provider.get_tracer("app").start_span(f"span {i}").end()
        """,
        receiver.endpoint,
    )
    assert [span.name for span in receiver.spans()] == ["span 0", "span 1", "span 2"]


def test_exit_waits_for_a_hung_collector_no_longer_than_the_export_timeout(receiver: FakeReceiver) -> None:
    with receiver.stalled():
        took, stderr = _exit(
            _start(
                """
                provider = TracerProvider()
                # A request would wait 10 minutes for the collector; shutdown waits 1 s
                processor = OTLPSpanProcessor(endpoint=endpoint, timeout=600, export_timeout_millis=1000)
                provider.add_span_processor(processor)
                provider.get_tracer("app").start_span("span").end()
                atexit.register(lambda: exit_started.append(time.monotonic()))
                """,
                receiver.endpoint,
            )
        )
    assert len(receiver.received) == 1
    # Measured in the process, from the first exit handler on: starting Python is no part of it
    [waited] = [float(line.split()[-1]) for line in stderr.splitlines() if line.startswith("EXIT WAITED")]
    assert 0.9 < waited < LIMIT / 2
    assert took < LIMIT / 2


@pytest.mark.parametrize("dropped", [False, True], ids=["kept", "garbage-collected"])
def test_without_shutdown_exit_does_not_wait_for_the_worker(receiver: FakeReceiver, dropped: bool) -> None:
    # The worker is in the middle of a request to a hung collector and more spans are queued; nothing joins it
    with receiver.stalled():
        process = _start(
            f"""
            provider = TracerProvider(shutdown_on_exit=False)
            processor = OTLPSpanProcessor(endpoint=endpoint, timeout=600, max_export_batch_size=1)
            provider.add_span_processor(processor)
            tracer = provider.get_tracer("app")
            tracer.start_span("sent").end()
            sys.stdin.readline()
            for i in range(100):
                tracer.start_span("queued").end()
            if {dropped}:
                import gc, weakref
                collected = weakref.ref(processor)
                del provider, processor, tracer
                gc.collect()
                assert collected() is None
            """,
            receiver.endpoint,
        )
        assert len(receiver.wait_for_requests(1, timeout=LIMIT)) == 1
        _exit(process, "go\n")


def test_sys_exit_in_a_thread_ends_only_that_thread(receiver: FakeReceiver) -> None:
    # The main thread is done first; the interpreter waits for the thread, then the provider shuts down
    _run(
        """
        provider = TracerProvider()
        provider.add_span_processor(OTLPSpanProcessor(endpoint=endpoint, schedule_delay_millis=600_000))
        tracer = provider.get_tracer("app")

        def work():
            time.sleep(0.2)
            tracer.start_span("from the thread").end()
            sys.exit(3)

        threading.Thread(target=work).start()
        tracer.start_span("from the main thread").end()
        """,
        receiver.endpoint,
    )
    assert sorted(span.name for span in receiver.spans()) == ["from the main thread", "from the thread"]


def test_a_daemon_thread_waiting_in_force_flush_does_not_hold_exit_up(receiver: FakeReceiver) -> None:
    with receiver.stalled():
        process = _start(
            """
            provider = TracerProvider()
            processor = OTLPSpanProcessor(endpoint=endpoint, timeout=600, export_timeout_millis=500)
            provider.add_span_processor(processor)
            provider.get_tracer("app").start_span("span").end()
            threading.Thread(target=processor.force_flush, args=(600_000,), daemon=True).start()
            sys.stdin.readline()
            """,
            receiver.endpoint,
        )
        assert len(receiver.wait_for_requests(1, timeout=LIMIT)) == 1
        # The process would exit in time anyway; what this checks is that the thread is not left to take the GIL
        # back inside Rust code during finalization (STILL INSIDE, before 3.14)
        took, _ = _exit(process, "go\n")
    assert took < LIMIT / 2


@pytest.mark.parametrize("shutdown_on_exit", [True, False])
@pytest.mark.parametrize("run", range(10))
def test_daemon_threads_still_ending_spans_at_exit(receiver: FakeReceiver, shutdown_on_exit: bool, run: int) -> None:
    # They go on through the provider's shutdown and into the interpreter's finalization, flushing too. Where they
    # stop depends on the scheduler, hence several runs
    took = _run(
        f"""
        provider = TracerProvider(shutdown_on_exit={shutdown_on_exit})
        processor = OTLPSpanProcessor(endpoint=endpoint, schedule_delay_millis=10)
        provider.add_span_processor(processor)
        tracer = provider.get_tracer("app")

        def emit():
            while True:
                tracer.start_span("span").end()

        def flush():
            while True:
                processor.force_flush()

        for target in [emit] * 8 + [flush] * 2:
            threading.Thread(target=target, daemon=True).start()
        time.sleep(0.3)
        sys.exit(0)
        """,
        receiver.endpoint,
    )
    assert receiver.spans()
    assert took < LIMIT / 2


def test_a_provider_older_than_the_import_still_exports_at_exit(receiver: FakeReceiver) -> None:
    # Its exit handler runs after fastotel's, which has closed the processor to the daemon threads by then; the
    # thread that exits still shuts it down and exports, waiting without the GIL while they go on
    _run(
        """
        tracer = provider.get_tracer("app")
        provider.add_span_processor(OTLPSpanProcessor(endpoint=endpoint, schedule_delay_millis=600_000))
        tracer.start_span("queued").end()

        def emit():
            while True:
                tracer.start_span("daemon").end()

        for _ in range(4):
            threading.Thread(target=emit, daemon=True).start()
        time.sleep(0.2)
        """,
        receiver.endpoint,
        before_import="""
        provider = TracerProvider()
        """,
    )
    assert "queued" in [span.name for span in receiver.spans()]
