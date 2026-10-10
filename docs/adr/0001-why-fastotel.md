# 0001. Why fastotel: the stock SDK's span export costs a CPU-bound service 11% of its throughput

- Status: accepted
- Date: 2026-10-10
- Ticket: #6 (step 0 of #1)

## Context

fastotel is worth building only if `BatchSpanProcessor` + the OTLP/HTTP exporter of the pure-Python SDK cost an
application something measurable. #6 sets the bar: **go** when the SDK with the OTLP exporter (scenario 3) costs a
CPU-bound workload at least **3%** of its throughput at **5k spans/s** on a build with the GIL, against the API with
no SDK (scenario 1); **no-go** below that.

## How it is measured

`make bench` runs the `pyperf` benchmarks of `bench/` on CPython 3.14 and 3.14t, then prints one table
(`bench/table.py`). The scenarios (`bench/scenarios.py`), each with the SDK's defaults (no `OTEL_*` variables: queue
2048, batch 512, delay 5 s, no compression):

1. **API, no SDK**: `NoOpTracerProvider`, what instrumentation does in a process without the SDK. Tracing off.
2. **SDK + `BatchSpanProcessor`, no-op exporter**: the cost of span creation and of the processor alone.
3. **SDK + `BatchSpanProcessor` + `OTLPSpanExporter`** (`opentelemetry-exporter-otlp-proto-http`), sending to a
   local sink (`bench/sink.py`) that runs in its own process, accepts `POST /v1/traces` and discards the body.

Measured in each (`bench/workload.py`):

- **Hot path, ns/span**: start a span, set 5 attributes (str, str, int, bool, float), end it, back to back. In the
  SDK scenarios this rate (70–115k spans/s) overflows the queue, so the exporter is always busy and most spans are
  dropped; the figure is the cost on the calling thread, with the exporter competing for it in scenario 3.
- **Workload, ops/s**: units of a pure-Python CPU-bound job (about 9 µs each) per second, while the same thread
  creates the hot-path span at 1k, 5k or 10k spans per second of wall time. The difference with scenario 1 is
  everything tracing costs the application: the hot path plus the exporter thread taking the GIL (or a core).
- **Exporter threads, CPU µs/span**: CPU time of the process minus that of the workload thread, during the
  workload runs, per span created. The SDK runs no other thread than the exporter's.

`pyperf` settings: 12 worker processes per benchmark, 1 warmup and 3 values each, a value lasting at least 1 s (a
few exports even at 1k spans/s). The cells show the mean and the relative standard deviation over the 36 values.

### Machine and builds

- Apple M2 Pro (6 performance + 4 efficiency cores), 16 GB, macOS 26.6.2, on AC power, under `caffeinate -i`. The
  machine was not idle: a code indexer kept one core busy and the load average was 3–4 throughout. Results vary by
  ±2–3% between values; the decision does not hinge on that.
- CPython 3.14.6 and 3.14.6 free-threaded (GIL off at run time), the python-build-standalone builds that uv installs.
- `opentelemetry-sdk` 1.45.1, `opentelemetry-exporter-otlp-proto-http` 1.45.1, `protobuf` 7.36.2, `requests`
  2.34.2, `pyperf` 2.10.0. `protobuf` runs on its `upb` C backend on 3.14 and on the **pure-Python** backend on 3.14t:
  it ships no free-threaded wheel.

## Results

`make bench`, 2026-10-10, 28 minutes:

| Python | Scenario | Hot path, ns/span | Workload, ops/s at 1k spans/s | Workload, ops/s at 5k spans/s | Workload, ops/s at 10k spans/s | Exporter threads, CPU µs/span |
|---|---|---|---|---|---|---|
| 3.14.6 (GIL on) | API, no SDK | 260 ±3.3% | 110,002 ±1.9% | 109,610 ±1.4% | 108,823 ±3.0% | 0.0 |
| 3.14.6 (GIL on) | SDK + BatchSpanProcessor, no-op exporter | 9,010 ±4.8% | 110,008 ±2.4% (+0.0%) | 105,769 ±2.1% (-3.5%) | 100,791 ±2.2% (-7.4%) | 0.5 |
| 3.14.6 (GIL on) | SDK + BatchSpanProcessor + OTLP/HTTP exporter | 13,366 ±2.5% | 108,349 ±2.7% (-1.5%) | 97,473 ±1.5% (-11.1%) | 88,124 ±2.5% (-19.0%) | 13.0 |
| 3.14.6t (GIL off) | API, no SDK | 282 ±1.9% | 113,051 ±4.0% | 114,220 ±1.8% | 113,790 ±2.0% | 0.0 |
| 3.14.6t (GIL off) | SDK + BatchSpanProcessor, no-op exporter | 8,600 ±2.6% | 113,494 ±2.3% (+0.4%) | 109,769 ±1.9% (-3.9%) | 103,407 ±2.0% (-9.1%) | 0.1 |
| 3.14.6t (GIL off) | SDK + BatchSpanProcessor + OTLP/HTTP exporter | 14,628 ±6.0% | 111,196 ±2.8% (-1.6%) | 96,276 ±15.3% (-15.7%) | 85,190 ±17.5% (-25.1%) | 120.5 |

Spans lost by the stock SDK in scenario 3, from a separate 10-second run of the same workload per rate with an
exporter that counts what it is handed:

| Python | 1k spans/s | 5k spans/s | 10k spans/s |
|---|---|---|---|
| 3.14.6 (GIL on) | 0% | 0% | 30.5% |
| 3.14.6t (GIL off) | 0% | 0% | 32.8% |

What the numbers say:

- At 5k spans/s on the GIL build, the full SDK pipeline costs the workload **11.1%** of its throughput. About a third
  of that is the SDK's own span creation and processor (3.5% in scenario 2: some 9 µs per span on the calling
  thread); the other two thirds are the exporter: 13 µs of CPU per span for encoding and sending, taken from the
  application while it holds the GIL.
- At 10k spans/s the exporter thread falls behind: the queue overflows and the SDK drops about 30% of the spans,
  logging `Queue full, dropping Span.` The -19.0% is a lower bound, since dropped spans are never encoded.
- The free-threaded build does not help today. Without a C backend, `protobuf` costs the exporter 120 µs of CPU per
  span, the exporter still drops spans at 10k/s, and the workload loses more (-15.7% at 5k spans/s, with a wide
  spread) than on the GIL build.
- At 1k spans/s the cost (-1.5%) is within the noise.

## Decision

**Go.** Scenario 3 costs the CPU-bound workload 11.1% of its throughput at 5k spans/s on CPython 3.14 with the GIL,
above the 3% bar of #6. fastotel proceeds to step 1 of #1: a Rust `OTLPSpanProcessor` that copies the span once in
`on_end` and does batching, encoding and export on a thread that never takes the GIL.

## Consequences

- The target for fastotel is the exporter's share: at 5k spans/s that is the gap between scenarios 2 and 3 (7.6
  points on the GIL build), plus the spans the SDK loses at 10k spans/s. The SDK's span creation (scenario 2) stays,
  since fastotel keeps the stock `TracerProvider`; `on_end` has to cost well under the 13 µs the exporter takes now.
- #18 re-runs `make bench` with fastotel's processor as a fourth entry in `SCENARIOS` (`bench/scenarios.py`); the
  harness, the table and the other scenarios stay as they are. It should also count lost spans, which the table
  does not show yet.
- These numbers come from a busy laptop; #18 compares on the same machine, run back to back, rather than against
  this table.
