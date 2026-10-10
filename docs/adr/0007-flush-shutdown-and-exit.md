# 0007. Flush, shutdown and exit: a queue closed by shutdown, the provider's exit handler, panics caught at the boundary

- Status: accepted
- Date: 2026-10-10
- Ticket: #12

## Context

#12 gives `force_flush` and `shutdown` the semantics of `BatchSpanProcessor`, asks that the process exit promptly
and cleanly whether or not the provider shuts down at exit, that a Rust panic never abort the interpreter, and that
many threads ending spans alongside `force_flush` and `shutdown`, on free-threaded 3.14t and on GIL builds, neither
deadlock nor lose nor duplicate spans. The reference is `opentelemetry-sdk` 1.45.1 read from its source:
`BatchSpanProcessor`, the `BatchProcessor` of `_shared_internal` it delegates to, `SynchronousMultiSpanProcessor` and
`TracerProvider.__init__`/`shutdown` with its `atexit` handler. ADR 0004 made `export_timeout_millis` the wait of
`shutdown`, ADR 0006 the `abandon` that ends the retries once that wait is over. The cost of `on_end` on the calling
thread is what fastotel's gain depends on (#18), so nothing here may add to it.

## Decision

### force_flush

- **Exports every span ended before the call**: the worker takes the request after every push that returned
  before it, and exports what the queue counts then (ADR 0004). A flush that comes during a retry waits for it,
  within its own timeout (ADR 0006).
- **False when the timeout passes first**; the export goes on, and the next flush waits for it. `BatchProcessor`
  1.45 ignores the timeout and exports on the caller's thread (open-telemetry/opentelemetry-python#4568); #12 asks
  for the timeout, as older SDKs honoured it.
- **False after `shutdown`**, as `BatchProcessor` 1.45 returns; SDKs up to 1.33 at least logged a warning and returned
  True. A flush that reaches the worker after a concurrent shutdown also returns False: the shutdown has exported
  what it would have.
- `timeout_millis` takes what `BatchSpanProcessor` and the SDK's multi-processors may pass: an int, a float, or
  None, which waits `export_timeout_millis`, as `BatchSpanProcessor` did while it honoured the timeout. Negative or
  NaN is no time, too large to hold no limit.

### shutdown

- **Idempotent**: the first call does the work, a concurrent or later one returns at once, as the SDK's.
- **Closes the queue.** The worker swaps the queue's count for a value that looks full, so a push that comes later
  fails the compare-and-swap it already makes (ADR 0004) and is dropped and counted: no other check on the
  application's thread. It then takes every span counted before, waiting for one whose push has counted it and not
  sent it yet, which takes no longer than that push, and exports them in batches within `export_timeout_millis`.
  Once that wait is over the worker is abandoned (ADR 0006) and counts what is left as failed. When the worker has
  answered, `shutdown` joins it.
- **`on_end` after it is a no-op**: one atomic load before the copy, the load `push` used to make after it, so the
  hot path costs the same (2.95 µs per span with 5 attributes before and after, release build, CPython 3.14, Apple
  M2 Pro, 1000 spans back to back then a flush, medians of 60 rounds). It reads `context.trace_flags` only, and counts
  a sampled span as dropped, as `BatchProcessor` counts it in its metrics (`already_shutdown`). `BatchProcessor` also
  logs it at info level; logging drops comes with #14.

So every sampled span handed to `on_end` is exported once, or counted: `dropped` (a full queue, or after shutdown),
`failed` (an export that failed, was abandoned or panicked), `rejected` (by the collector), or `panics` (a panic in
its copy). Only a span the copy cannot read (a Python exception, ADR 0002) is not counted yet; #14 reports it.
`tests/test_threads.py` checks the sum with 32 threads ending spans, 4 flushing, and a shutdown midway; Rust tests in
`pipeline.rs` check the closed queue, including a push that shutdown overtakes between counting and sending.

### Exit: the provider's handler, and nothing of fastotel's own

`TracerProvider(shutdown_on_exit=True)`, the default, registers `atexit` to shut its processors down: queued spans
leave then, within `export_timeout_millis`. fastotel registers no handler of its own, as `BatchSpanProcessor` does
not: a provider created with `shutdown_on_exit=False` has opted out of waiting at exit, and a handler would make the
process wait up to the export timeout against that choice.

The worker is a native thread that `threading` does not know, so neither the interpreter's wait for non-daemon
threads nor its finalization waits for it, and nothing joins it at exit; the process ends with it. Daemon threads
still ending spans after the provider's shutdown cost one atomic load each, and `force_flush` returns False for
them at once. Hence:

| How the process ends | Spans still queued |
| --- | --- |
| normally, `sys.exit` in the main thread, or an uncaught exception, with the provider's exit handler (the default) | exported, waiting up to `export_timeout_millis` |
| the same with `TracerProvider(shutdown_on_exit=False)` and no `shutdown()` | lost; exit does not wait |
| `sys.exit` in another thread | that thread ends; the process goes on and ends as above |
| `os._exit`, a fatal signal, `SIGKILL` | lost: no exit handler runs |

The tests run each case in a process of its own with a time limit (`tests/test_exit.py`), with a hung collector
where the waiting matters.

### Panics

A panic never unwinds into Python or past the worker's loop:

- **At the boundary**: `on_end`, `force_flush` and `shutdown` catch a panic (`catch_unwind`), count it in `panics`
  and log it at error level on the `fastotel` logger. `on_end` returns normally and loses the span: it runs inside
  the application's `span.end()`, where PyO3 would raise `PanicException`, a `BaseException`. `force_flush` and
  `shutdown` return False.
- **On the worker**: a panic while encoding or sending a batch loses that batch, counted as failed, and the worker
  goes on with the next one. A panic elsewhere in its loop ends the worker, closes the queue and counts what is in
  it as failed, so later spans are dropped and counted and `force_flush` returns False. The worker cannot reach
  Python, so it keeps up to 16 messages, and the next `force_flush` or `shutdown` logs them; #14 hands them over as
  they happen.
- The locks recover from poisoning, so that a panic caught once does not turn into a panic per call. The process's
  panic hook, which prints the panic to stderr, belongs to the application and is left alone.
- The native module exports PyO3's `PanicException`: Python code that raises it into Rust resumes the panic there,
  which is how the tests make the copy panic.

### Where fastotel differs from BatchSpanProcessor

- `force_flush` honours its timeout; None waits `export_timeout_millis`.
- `shutdown` waits `export_timeout_millis`, where `BatchProcessor` waits 30 s (ADR 0004), and stops the exports once
  the wait is over (ADR 0006).
- A span that comes after shutdown is counted but not logged, until #14.

## Consequences

- #13 restarts the worker in a forked child: the closed queue and the shut-down flag belong to the parent's
  pipeline, and a child's new worker starts with an open queue unless the parent had shut down.
- #14 makes the counters public in `stats()`, `panics` among them; `dropped` counts both a full queue and spans after
  shutdown, which `stats()` may split. Its hand-over to the `fastotel` logger replaces the logging of the worker's
  panics at the next flush or shutdown.
- `Pipeline::push` no longer checks the shut-down flag; its caller does, before copying the span
  (`Pipeline::is_shut_down`), and counts what it does not push (`Pipeline::drop_span`).
