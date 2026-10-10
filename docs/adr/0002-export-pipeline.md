# 0002. The export pipeline: a crate without PyO3, one worker thread, opentelemetry-proto and ureq with rustls

- Status: accepted
- Date: 2026-10-10
- Ticket: #7 (the tracer bullet of #5)

## Context

#7 is the first span to go all the way through fastotel: `OTLPSpanProcessor.on_end` copies the span into Rust,
and a Rust thread encodes it as OTLP protobuf and posts it to `/v1/traces`. The later tickets of #5 widen this
pipeline (#8 every field, #9 batching and the queue, #10 configuration, #11 gzip, retries and TLS, #12 flush and
shutdown, #13 fork, #14 diagnostics), so the choices here are the ones they build on. #7 sets the constraints:

- the worker never takes the GIL, `on_end` holds it only for the copy, and the code makes that checkable;
- the wheels keep building and passing on the 8 platforms of `wheels.yml`, musllinux and Windows arm64 included,
  so TLS is rustls, never OpenSSL;
- no thread or runtime starts at import or construction, so that a fork does not break it (#13).

## Decision

### Two crates: the pipeline cannot reach Python

- `crates/export` (`fastotel-export`) holds the span model (`SpanData`), the encoder and the pipeline: the queue,
  the worker thread and the HTTP client. **It does not depend on PyO3**, so nothing in it can take the GIL: the
  compiler checks the first constraint, and `make lint` fails when the crate's dependency tree reaches PyO3. Its
  tests run with `cargo test` and no Python.
- The extension module (`src/lib.rs`) is the only code that touches Python. `Processor.on_end` copies the
  `ReadableSpan` into a `SpanData` (owned Rust data, nothing from Python in it) and pushes it onto the queue with a
  `try_send` that never blocks: that is all it does with the GIL. `force_flush` and `shutdown` release the GIL
  (`Python::detach`) while they wait for the worker.
- The Python class `fastotel.OTLPSpanProcessor` subclasses the SDK's `SpanProcessor` and forwards each call to the
  native `fastotel._fastotel.Processor` in one call.

### One worker thread per processor, started by the first span

- The pipeline starts its thread, `fastotel-export`, on the first span it receives (a `OnceLock`); importing
  fastotel or creating a processor starts nothing. A test on Linux reads the threads of the process from `/proc`.
- No async runtime. #1 mentioned tokio, but the pipeline does what `BatchSpanProcessor` does, one export at a time,
  for which a blocking HTTP client on a plain thread is enough; a thread is also simpler than a runtime to park
  and restart around a fork (#13).
- The queue is a bounded `crossbeam-channel` of 2048 spans; a full queue drops the span. Flush and shutdown go
  through a second, unbounded channel, so they never wait behind a full queue; the worker then drains the queue and
  exports what it holds. A batch leaves at 512 spans or every 5 s, a request has 10 s. These are the SDK's defaults,
  fixed until #9 and #10 read them from the environment.
- `shutdown()` waits up to 30 s (the default of `OTEL_BSP_EXPORT_TIMEOUT`) for the last export, is idempotent, and
  keeps a worker from starting afterwards; spans ended after it are ignored. #12 gives both their full semantics.

### OTLP types from opentelemetry-proto, no protoc

- `opentelemetry-proto` 0.33 with the features `gen-tonic-messages` and `trace` gives the prost 0.14 types of
  `ExportTraceServiceRequest`. Its generated code is checked in to the crate, so the build needs no `protoc` on any
  platform. It also compiles the `opentelemetry` and `opentelemetry_sdk` crates it depends on: pure Rust, a cost
  at build time only.

### HTTP through ureq, TLS through rustls with ring and the OS trust store

- `ureq` 3, a blocking client with a connection pool, without its default features: `rustls` (rustls with the
  `ring` provider) and `platform-verifier` (`rustls-platform-verifier`, which verifies against the trust store of
  the OS, so corporate CAs work, as #11 asks; the reference exporter uses certifi through requests instead). No
  OpenSSL, and not `aws-lc-rs`, which needs CMake or NASM on some targets; `ring` builds with the C compiler of the
  target. `https://` endpoints are verified against the OS trust store, untested until #11, which also brings
  `certificate_file` and mTLS.
- A failed request or a non-2xx answer drops the batch for now: retries are #11, counting and logging #14.

### Public API

- `OTLPSpanProcessor(*, endpoint=None)`. **`endpoint` is the URL spans are posted to, `/v1/traces` included,** as
  for the reference `OTLPSpanExporter(endpoint=...)`, and its default is `http://localhost:4318/v1/traces`. #7 wrote
  `POST <endpoint>/v1/traces` with a default of `http://localhost:4318`, but #10 asks for the reference's argument
  names with the reference's meaning, so that moving over changes only the import; taking that meaning now spares
  a breaking change in #10. `None` stands for the default, which #10 extends with `OTEL_EXPORTER_OTLP_*`.
- The arguments are keyword-only: #9 and #10 add the names of `BatchSpanProcessor` and of the reference exporter,
  and positions would not mean the same thing for both.

### Behaviour, where it matters later

- Only sampled spans are exported, as by `BatchSpanProcessor`: a span recorded but not sampled reaches `on_end` and
  is ignored there.
- `on_end` runs inside the application's `span.end()`: a span it cannot copy is dropped instead of raising an
  `Exception` there; `KeyboardInterrupt` and `SystemExit` still go through.
- A child forked after the first span inherits the channels of the parent's worker but not its thread. Until #13
  starts a worker in the child, the pipeline remembers the pid that started it and in another process ignores
  spans, flushes and shutdowns and leaks the channels rather than dropping them: waking the parent's worker from the
  child traps in libdispatch on macOS, and on Linux `shutdown()` would wait 30 s for an answer. Spans of such a
  child are not exported yet; a fork before the first span (gunicorn `--preload` without spans at import) works.
- The fields of this slice: trace id, span id, parent span id, name, kind, start and end time, string attributes,
  resource attributes, scope name and version. Attributes of other types are left out until #8.
- A resource or a scope is copied once per Python object, not with every span: the processor keeps up to 64 of
  each by identity, holding a reference so that the address is not reused. The encoder groups spans by equal
  resource and then equal scope, as the reference groups them, so tracers created anew per request (as old SDKs
  do with `get_tracer`) still share one `ScopeSpans`.
- `opentelemetry-api` and `opentelemetry-sdk` are runtime dependencies from 1.16: `ReadableSpan` has
  `instrumentation_scope` from 1.11, but before 1.16 the API imports `pkg_resources`, which current setuptools no
  longer ships. The `lowest` job of CI runs the tests with the lowest versions allowed.

A rough indication, not a benchmark: `on_end` of a span with 5 attributes takes about 1.9 µs on the calling thread
(release build, CPython 3.14, Apple M2 Pro), against the 16 µs of CPU per span the SDK's exporter takes from the
application (ADR 0001). #18 measures it properly.

## Consequences

- #8 widens `SpanData` and `Attributes` in `crates/export/src/span.rs`, the copy in `src/lib.rs` and the encoder;
  the compatibility test reuses the fake receiver of `tests/receiver.py` (the `receiver` fixture).
- #9 and #10 fill `fastotel_export::Config` from the environment and the constructor; #9 counts the spans the full
  queue drops in `Pipeline::push`.
- #11 changes `export` in `crates/export/src/pipeline.rs` (gzip, retries) and the `TlsConfig` the worker builds.
- #13 needs a pipeline it can restart in the child: the `OnceLock` that holds the worker cannot be reset and gives
  way to a state the fork handlers can replace, and the pid check of `Worker::in_this_process` turns into a restart.
- #14 needs a way for the worker to hand messages to the `fastotel` logger without the GIL; nothing reports yet.
