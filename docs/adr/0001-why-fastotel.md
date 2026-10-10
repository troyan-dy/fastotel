# 0001. Why fastotel: the stock SDK's span export costs a CPU-bound service 9–11% of its throughput

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
(`bench/table.py`) and the decision. The scenarios (`bench/scenarios.py`), each with the SDK's defaults (no `OTEL_*`
variables: queue 2048, batch 512, delay 5 s, no compression):

1. **API, no SDK**: `NoOpTracerProvider`, the tracer instrumentation ends up calling in a process without the SDK.
   Tracing off.
2. **SDK + `BatchSpanProcessor`, no-op exporter**: the cost of span creation and of the processor alone.
3. **SDK + `BatchSpanProcessor` + `OTLPSpanExporter`** (`opentelemetry-exporter-otlp-proto-http`), sending to a
   local sink (`bench/sink.py`) that runs in its own process, accepts `POST /v1/traces`, answers 200 at once and
   counts the spans on another thread.

Measured in each (`bench/workload.py`, `bench/run.py`):

- **Hot path, ns/span**: start a span, set 5 attributes (str, str, int, bool, float), end it, back to back. In the
  SDK scenarios this rate (75–130k spans/s) overflows the queue, so the exporter is always busy and most spans are
  dropped; the figure is the cost on the calling thread, with the exporter competing for it in scenario 3.
- **Workload, ops/s**: units of a pure-Python CPU-bound job (about 9 µs each) per second, while the same thread
  creates the hot-path span at 1k, 5k or 10k spans per second of wall time. The difference with scenario 1 is
  everything tracing costs the application: the hot path plus the exporter thread taking the GIL (or a core).
- **Exporter threads, CPU µs/span at 5k spans/s**: CPU time of the process minus that of the workload thread, over
  every workload run at 5k spans/s (pyperf's calibration and warmups included), per span created. The SDK runs no
  other thread than the exporter's. Not at 10k spans/s: there the SDK drops spans it never encodes.
- **Spans lost**: after the pyperf benchmarks, each scenario runs the workload for 10 s per rate, then shuts the
  provider down, which exports what is left in the queue; the spans the sink did not receive are lost. Only
  scenario 3 sends spans to the sink.

`pyperf` settings: 12 worker processes per benchmark, 1 warmup and 3 values each, a value lasting at least 1 s (a
few exports even at 1k spans/s). The cells show the mean and the relative standard deviation over the 36 values.

### Machine and builds

- Apple M2 Pro (6 performance + 4 efficiency cores), 16 GB, macOS 26.6.2, on AC power, under `caffeinate -i`. The
  machine was not idle: a code indexer and system daemons kept about one core busy, load average 2.5–4.5.
- CPython 3.14.6 and 3.14.6 free-threaded (GIL off at run time), the python-build-standalone builds that uv installs.
- `opentelemetry-sdk` 1.45.1, `opentelemetry-exporter-otlp-proto-http` 1.45.1, `protobuf` 7.36.2, `requests`
  2.34.2, `pyperf` 2.10.0. `protobuf` runs on its `upb` C backend on 3.14 and on the **pure-Python** backend on 3.14t:
  it ships no free-threaded wheel.

## Results

`make bench`, 2026-10-10, 30 minutes:

| Python | Scenario | Hot path, ns/span | Workload, ops/s at 1k spans/s | Workload, ops/s at 5k spans/s | Workload, ops/s at 10k spans/s | Exporter threads, CPU µs/span at 5k spans/s | Spans lost at 1k / 5k / 10k spans/s |
|---|---|---|---|---|---|---|---|
| 3.14.6 (GIL on) | API, no SDK | 262 ±1.5% | 109,316 ±1.6% | 109,961 ±1.5% | 111,023 ±2.0% | 0.0 |  |
| 3.14.6 (GIL on) | SDK + BatchSpanProcessor, no-op exporter | 8,611 ±2.6% | 110,618 ±2.9% (+1.2%) | 109,786 ±1.5% (-0.2%) | 105,053 ±1.3% (-5.4%) | 0.4 |  |
| 3.14.6 (GIL on) | SDK + BatchSpanProcessor + OTLP/HTTP exporter | 12,309 ±1.1% | 111,439 ±2.1% (+1.9%) | 100,472 ±2.0% (-8.6%) | 90,622 ±2.0% (-18.4%) | 16.2 | 0.0% / 0.0% / 30.2% |
| 3.14.6t (GIL off) | API, no SDK | 274 ±3.0% | 120,126 ±1.5% | 120,052 ±0.9% | 119,099 ±3.5% | 0.0 |  |
| 3.14.6t (GIL off) | SDK + BatchSpanProcessor, no-op exporter | 7,867 ±1.1% | 118,278 ±3.5% (-1.5%) | 115,289 ±1.0% (-4.0%) | 109,993 ±2.3% (-7.6%) | 0.1 |  |
| 3.14.6t (GIL off) | SDK + BatchSpanProcessor + OTLP/HTTP exporter | 12,897 ±2.2% | 117,543 ±1.7% (-2.1%) | 109,185 ±3.0% (-9.1%) | 99,212 ±2.9% (-16.7%) | 145.5 | 0.0% / 0.0% / 33.1% |

An earlier run the same day, with the same scenarios and an earlier table that summed the exporter's CPU over all
rates, gave scenario 3 on the GIL build -1.5% / **-11.1%** / -19.0% at 1k / 5k / 10k spans/s, and scenario 2 -3.5% at
5k spans/s. Between the two runs the cells move by up to 3 points, which is the noise of this machine.

What the numbers say:

- At 5k spans/s on the GIL build, the full SDK pipeline costs the workload **8.6%** of its throughput (11.1% in the
  earlier run). The exporter takes 16 µs of CPU per span for encoding and sending, on the application's GIL; the
  SDK's own span creation and processor, about 9 µs per span on the calling thread, cost 0–3.5% at that rate.
- At 10k spans/s the exporter thread falls behind: the queue overflows and the SDK drops about 30% of the spans
  on both builds, logging `Queue full, dropping Span.` The -18.4% is a lower bound, since dropped spans are never
  encoded.
- The free-threaded build does not help today. Without a C backend, `protobuf` costs the exporter 146 µs of CPU per
  span, the exporter still drops a third of the spans at 10k/s, and the workload loses as much (-9.1% at 5k spans/s)
  as on the GIL build.
- At 1k spans/s the cost is within the noise.

## Decision

**Go.** Scenario 3 costs the CPU-bound workload 8.6% of its throughput at 5k spans/s on CPython 3.14 with the GIL
(11.1% in an earlier run), well above the 3% bar of #6. fastotel proceeds to step 1 of #1: a Rust
`OTLPSpanProcessor` that copies the span once in `on_end` and does batching, encoding and export on a thread that
never takes the GIL.

## Consequences

- The target for fastotel is the exporter's share: at 5k spans/s that is the gap between scenarios 2 and 3 (5–8
  points on the GIL build), plus the third of the spans the SDK loses at 10k spans/s. The SDK's span creation
  (scenario 2) stays, since fastotel keeps the stock `TracerProvider`; `on_end` has to cost well under the 16 µs of
  CPU the exporter takes per span now, and fastotel must not lose spans at 10k spans/s.
- #18 re-runs `make bench` with fastotel's processor as a fourth entry in `SCENARIOS` (`bench/scenarios.py`); the
  harness, the table and the other scenarios stay as they are. Lost spans are counted at the sink, so they show up
  for any scenario that sends OTLP to it.
- These numbers come from a busy laptop and move by up to 3 points between runs; #18 compares on the same machine,
  run back to back, rather than against this table.
