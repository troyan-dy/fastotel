# 0004. Batching and the queue: BatchSpanProcessor's settings, a counted queue and a worker woken by full batches

- Status: accepted
- Date: 2026-10-10
- Ticket: #9

## Context

#9 gives `OTLPSpanProcessor` the settings of `BatchSpanProcessor`, `max_queue_size`, `schedule_delay_millis`,
`max_export_batch_size` and `export_timeout_millis`, with its `OTEL_BSP_*` variables, defaults, parsing and checks,
and asks that a full queue drop and count spans without `on_end` ever blocking, and that memory stay bounded by the
queue while the collector is down. The reference is `BatchSpanProcessor` and the `BatchProcessor` it delegates to
in `opentelemetry-sdk` 1.45.1, read from the source. The measurement after #8 (in #18) found that the calling
thread's cost in `on_end` is what fastotel's gain depends on, so nothing here may add to it.

## Decision

### The settings are read in Python, as BatchSpanProcessor reads them

- `OTLPSpanProcessor(*, endpoint=None, max_queue_size=None, schedule_delay_millis=None, max_export_batch_size=None,
  export_timeout_millis=None)`: the names of `BatchSpanProcessor`, keyword-only as `endpoint` (ADR 0002). An
  argument left at None comes from its variable, `OTEL_BSP_MAX_QUEUE_SIZE` (2048), `OTEL_BSP_SCHEDULE_DELAY`
  (5000 ms), `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` (512) or `OTEL_BSP_EXPORT_TIMEOUT` (30000 ms); a variable is read only
  when its argument is None.
- A variable is parsed with `int()`, as the SDK does, so ` 1_000 ` and `+7` count and `1.5` does not. A value
  `int()` refuses gives the default and the SDK's error log, with its message and traceback, on the `fastotel`
  logger, which #14 builds on. Older SDKs, 1.16 among them, raise `ValueError` instead; fastotel follows the
  current ones, and the comparison test skips those cases on the `lowest` CI job.
- The checks of `BatchSpanProcessor` with its messages, raised as `ValueError` from the constructor: the queue,
  the delay and the batch size must be positive, and the batch no larger than the queue. The export timeout is not
  checked, as in the SDK.
- This runs once, in the constructor; the native `Processor` takes the resolved values. `tests/test_batching.py`
  builds a `BatchSpanProcessor` next to an `OTLPSpanProcessor` from the same variables and arguments and compares
  the settings, the error and the log records, for every variable and its invalid values.

### What the settings do

- **A batch leaves** when `max_export_batch_size` spans are queued, at once, or `schedule_delay_millis` after the
  previous export, whichever comes first; the delay starts over after every export, as the SDK's worker waits the
  delay anew after each wake-up. When the delay expires, everything queued leaves, in requests of at most the batch
  size; the SDK sends one batch then and leaves a remainder smaller than a batch for the next round.
- **The queue** holds `max_queue_size` spans waiting for export; the batch being sent is out of it, as the SDK pops
  a batch from its deque before exporting it. A span that finds it full is dropped and counted. So while the
  collector hangs, fastotel holds at most `max_queue_size` spans plus the batch in flight, as the SDK does: with the
  defaults, about 2.5k spans, a few MB (measured in a subprocess by `test_memory_stays_bounded_...`).
- **`export_timeout_millis`** is how long `shutdown()` waits for the last export, which ADR 0002 fixed at 30 s,
  that variable's default, and which #12 asks to be "within the export timeout". The SDK reads the variable and
  does not use it ("No way currently to pass timeout to export", open-telemetry/opentelemetry-python#4555). It does
  not limit a single request: that is the exporter's timeout, `OTEL_EXPORTER_OTLP_TIMEOUT` (#10), 10 s for now.
  Negative is no wait; too large to hold, forever.

### The queue: an unbounded channel, bounded by a counter, and a worker woken by full batches

ADR 0002 used a bounded `crossbeam-channel` of 2048 that the worker blocked on. Two things change:

- A bounded channel allocates all its slots when it is created, a few hundred bytes each: with a
  `max_queue_size` of a million that is hundreds of MB at the first span. The spans now go through an unbounded
  channel, which allocates blocks as it fills and frees them as it empties, and an atomic counter of queued spans
  bounds it: `push` adds one and drops the span when that exceeds the queue size; the worker subtracts what it
  takes out.
- The worker no longer waits on the span channel. It waits on its control channel (flush, shutdown) and on a
  one-slot wake-up channel, with the time left until the delay expires; the push that brings the count to the
  batch size sends the wake-up, as the SDK sets its worker's event only then. Before, every push woke the parked
  worker, a system call on the application's thread for every span. `on_end` of a span with 5 attributes went
  from 3.4–3.9 µs to 2.8–3.1 µs (release build, CPython 3.14, Apple M2 Pro, 1000 spans back to back then a
  flush, medians of 60 rounds); a rough figure, which #18 measures properly.

The drop count lives in the pipeline (`Pipeline::dropped_spans`) and reaches Python as the native
`Processor.dropped_spans()`. There is no public counter yet: `OTLPSpanProcessor.stats()` of #14 exposes it with
the others, and logging the drop, which the SDK does with every dropped span, comes with #14's rate-limited logger.

### Where fastotel differs from BatchSpanProcessor

- **A full queue drops the new span**, as the OTel specification says and ADR 0002 already did; the SDK's deque
  drops the oldest one. Either way the count is the overflow.
- **A schedule delay of NaN** is refused with the SDK's message (`schedule_delay_millis must be positive.`). The
  SDK's check `<= 0` lets NaN through, and its worker then waits no time at all and spins. Only an argument can be
  NaN; a variable is an int.
- **The remainder after the delay** leaves with it, as above, rather than one batch per round.

## Consequences

- #10 adds the exporter's arguments to the same constructor and its variables next to these; it keeps the order
  (argument, then variable, then default) and the comparison against the reference object.
- #12 builds `shutdown` and `force_flush` on `Config::export_timeout` and the counter: `force_flush` exports the
  spans counted when the worker takes the request, which includes every push that returned before it.
- #14 turns `dropped_spans` into `stats()` and adds the queue length, which is the counter.
- #13 restarts the worker in a forked child: the counter and the channels go with the `Worker` it replaces.
- The worker blocks on `recv` for a span that is counted but not yet sent, which lasts as long as a push between
  its two steps; a push never fails between them.
