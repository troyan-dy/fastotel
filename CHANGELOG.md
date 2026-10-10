# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.7.0] - 2026-10-10

### Added

- `OTLPSpanProcessor` reads the exporter configuration of the reference `OTLPSpanExporter` (#10): the arguments
  `endpoint`, `headers`, `timeout`, `compression`, `certificate_file`, `client_key_file` and
  `client_certificate_file`, or the `OTEL_EXPORTER_OTLP_TRACES_*` and `OTEL_EXPORTER_OTLP_*` variables, with the
  reference's precedence, header parsing and percent-decoding, defaults and warnings, compared with the reference
  in the tests. Headers and the timeout apply to every request; compression and the TLS files are read and applied
  with #11.
- Requests carry `User-Agent: fastotel/<version>`.
- `docs/adr/0005-exporter-configuration.md`: how the configuration is read and where fastotel differs from the
  reference: a header HTTP cannot carry raises `ValueError` at construction, and `compression` also takes a string.

## [0.6.0] - 2026-10-10

### Added

- `OTLPSpanProcessor` takes the batching settings of `BatchSpanProcessor` (#9): `max_queue_size`,
  `schedule_delay_millis`, `max_export_batch_size` and `export_timeout_millis`, or the `OTEL_BSP_*` variables, with
  the SDK's defaults, parsing, error log and checks, compared with `BatchSpanProcessor` in the tests. A batch leaves
  when it is full or when the delay expires; `shutdown()` waits up to the export timeout.
- A full queue drops the span and counts it, without blocking `on_end`; while the collector hangs, fastotel holds
  at most the queue and the batch being sent.
- `docs/adr/0004-batching-and-the-queue.md`: the settings, the queue and where fastotel differs from
  `BatchSpanProcessor`.

### Changed

- The worker is woken by a full batch or its timer instead of by every span, so `on_end` no longer pays for waking
  a thread: about 0.6 µs less per span. The queue allocates as it fills, so a large `max_queue_size` costs no
  memory up front.

## [0.5.0] - 2026-10-10

### Added

- `OTLPSpanProcessor` sends every field of a span, encoded as the reference `OTLPSpanExporter` encodes it (#8):
  attributes of every type (bool, int, float, str, bytes, None, sequences and mappings, nested) on the span, the
  resource, the scope, events and links; events, links, status, trace state, the is-remote flags, the dropped
  counts, schema URLs and scope attributes. Spans group into resources and scopes as the reference groups them.
- A compatibility test: the same spans, hand-picked and generated with `hypothesis`, go through the reference
  exporter and through fastotel, and the decoded requests must be equal.
- `docs/adr/0003-span-encoding.md`: the encoding, the values OTLP cannot express, and the two cases where
  fastotel differs from the reference: an attribute nested deeper than protobuf decoders accept is left out, and
  a span whose name or other string field is not valid UTF-8 is dropped alone rather than with its batch.

## [0.4.0] - 2026-10-10

### Added

- `fastotel.OTLPSpanProcessor`, a `SpanProcessor` that sends finished spans over OTLP/HTTP protobuf in place of
  `BatchSpanProcessor` with `OTLPSpanExporter` (#7). `on_end` copies the span into Rust; a native thread, started by
  the first span, batches, encodes and posts them to `endpoint` (default `http://localhost:4318/v1/traces`) without
  taking the GIL. This first slice sends the ids, name, kind, times and string attributes of a span, the string
  attributes of its resource and the name and version of its scope; the other fields, the `OTEL_*` variables,
  retries and gzip follow in the tickets of #5.
- `docs/adr/0002-export-pipeline.md`: the crates and the thread model the pipeline is built on.
- Dependencies on `opentelemetry-api` and `opentelemetry-sdk` 1.16 or newer.

## [0.3.0] - 2026-10-10

### Added

- `make bench`: `pyperf` scenarios in `bench/` that measure what the stock SDK costs an application, the API
  with no SDK against `BatchSpanProcessor` with a no-op exporter and with the OTLP/HTTP exporter sending to a
  local sink (#6). They report ns per span on the hot path, the throughput of a CPU-bound workload at 1k, 5k
  and 10k spans/s, the CPU time of the exporter thread and the share of spans lost, on a GIL and a free-threaded
  build. The package itself is unchanged; the benchmark dependencies live in the `bench` dependency group.
- `docs/adr/0001-why-fastotel.md`: the numbers and the go/no-go decision for building fastotel.

## [0.2.0] - 2026-10-09

### Added

- Wheels for the free-threaded CPython 3.14 (`cp314t`) on every platform, next to the `abi3` ones (#3). The
  native module declares that it does not need the GIL, so importing it keeps the GIL off.
- CI runs the tests on CPython 3.11, 3.12, 3.13, 3.14, 3.14t and on the 3.15 and 3.15t pre-releases; every
  wheel is tested on its own runner, the `abi3` one on 3.11, 3.14 and 3.15.

## [0.1.0] - 2026-10-09

### Added

- The package skeleton: a PyO3 extension module built with maturin, `fastotel.__version__`, type stubs and
  `py.typed`. Nothing replaces the OpenTelemetry SDK yet; see #1 for the plan.
- `abi3` wheels for CPython 3.11 and newer on Linux (manylinux and musllinux, x86_64 and aarch64), macOS
  (x86_64 and arm64) and Windows (x64 and arm64), plus the sdist.
- Releases: every version that reaches `master` is published to PyPI through Trusted Publishing, tagged and
  given release notes from this file.

[Unreleased]: https://github.com/troyan-dy/fastotel/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/troyan-dy/fastotel/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/troyan-dy/fastotel/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/troyan-dy/fastotel/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/troyan-dy/fastotel/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/troyan-dy/fastotel/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/troyan-dy/fastotel/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/troyan-dy/fastotel/releases/tag/v0.1.0
