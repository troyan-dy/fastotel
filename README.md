# fastotel

[![PyPI](https://img.shields.io/pypi/v/fastotel)](https://pypi.org/project/fastotel/)
[![Python](https://img.shields.io/pypi/pyversions/fastotel)](https://pypi.org/project/fastotel/)
[![CI](https://github.com/troyan-dy/fastotel/actions/workflows/ci.yml/badge.svg)](https://github.com/troyan-dy/fastotel/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/fastotel)](https://github.com/troyan-dy/fastotel/blob/master/LICENSE)

A Rust-backed drop-in for the OpenTelemetry Python SDK that takes tracing overhead off the request path.

> **Status: pre-alpha.** `OTLPSpanProcessor` sends every field of a span over OTLP/HTTP, encoded as
> `OTLPSpanExporter` encodes it, configured as it is configured, compressed, through TLS and with its retries, and
> batches as `BatchSpanProcessor` does, but a process forked after the first span exports nothing from the child
> yet. The road to 1.0 is in
> [#5](https://github.com/troyan-dy/fastotel/issues/5).

## Why

Instrumentation packages (FastAPI, httpx, SQLAlchemy, aiokafka, …) depend only on `opentelemetry-api`.
fastotel replaces parts of `opentelemetry-sdk` behind that API, so existing instrumentation keeps working.

In the pure-Python SDK, the `BatchSpanProcessor` worker thread encodes OTLP protobuf, gzips and sends spans
while holding the GIL, so it competes for CPU with request handling. In Rust, batching, encoding, compression
and export run on a thread that never takes the GIL.

## Installation

```bash
pip install fastotel
```

Wheels are built for Linux (glibc and musl), macOS and Windows, x86_64 and arm64:

| Python | Wheel | Tested in CI |
| --- | --- | --- |
| CPython 3.11, 3.12, 3.13, 3.14 | one `abi3` wheel per platform, which also covers later versions | yes |
| CPython 3.14t, free-threaded | `cp314t`; the GIL stays off after import | yes |
| CPython 3.15 (pre-release) | the `abi3` wheel | yes |
| CPython 3.15t (pre-release) | none yet: builds from the sdist; a wheel ships with 3.15.0 | yes, from source |

CPython 3.10 reached end of life on 2026-10-01 and is not supported. Free-threaded 3.13t is not supported
either: it was experimental, and PyO3 builds free-threaded extensions from 3.14 on.

## Usage

fastotel replaces `BatchSpanProcessor` with `OTLPSpanExporter`; the SDK keeps creating spans:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from fastotel import OTLPSpanProcessor

provider = TracerProvider()
provider.add_span_processor(OTLPSpanProcessor())  # reads OTEL_EXPORTER_OTLP_* and OTEL_BSP_*
trace.set_tracer_provider(provider)
```

`on_end` copies the span into Rust and returns; a native thread, started by the first span, batches,
encodes and sends spans without taking the GIL. As with `BatchSpanProcessor`, only sampled spans are exported. The
requests decode to what `OTLPSpanExporter` sends for the same spans; where they differ is in
[ADR 0003](https://github.com/troyan-dy/fastotel/blob/master/docs/adr/0003-span-encoding.md).

### Exporter

The arguments and variables of `OTLPSpanExporter` (`opentelemetry-exporter-otlp-proto-http`), with its defaults,
precedence and parsing: an argument overrides the `OTEL_EXPORTER_OTLP_TRACES_*` variable, which overrides the
`OTEL_EXPORTER_OTLP_*` one; an empty variable counts as unset. A timeout or a compression that does not parse gives
the default, and a header entry that does not parse is skipped, each with the reference's warning on the
`fastotel` logger.

| Argument | Variables | Default | What it does |
| --- | --- | --- | --- |
| `endpoint` | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, as it is; `OTEL_EXPORTER_OTLP_ENDPOINT`, with `/v1/traces` appended | `http://localhost:4318/v1/traces` | the URL spans are posted to; the argument, like the traces variable, includes the path |
| `headers` | `OTEL_EXPORTER_OTLP_TRACES_HEADERS`, `OTEL_EXPORTER_OTLP_HEADERS` | `Content-Type: application/x-protobuf`, `User-Agent: fastotel/<version>` | sent with every request; a variable holds `name=value` pairs separated by commas, URL-encoded (`Authorization=Basic%20dXNlcg%3D%3D`); names are lower-cased, and the argument's headers are merged over the variable's |
| `timeout` | `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`, `OTEL_EXPORTER_OTLP_TIMEOUT` | 10 | seconds the export of a batch may take, retries included |
| `compression` | `OTEL_EXPORTER_OTLP_TRACES_COMPRESSION`, `OTEL_EXPORTER_OTLP_COMPRESSION` | `none` | `gzip`, `deflate` or `none`; the argument is the reference's `Compression` or a string. Adds `Content-Encoding` unless the headers carry one |
| `certificate_file` | `OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE`, `OTEL_EXPORTER_OTLP_CERTIFICATE` | the OS trust store | a PEM file of the CAs to verify an `https://` collector with, in place of the OS trust store |
| `client_certificate_file` | `OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE`, `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | none | the client certificate chain for mTLS, PEM; without `client_key_file` the file holds the key too |
| `client_key_file` | `OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY`, `OTEL_EXPORTER_OTLP_CLIENT_KEY` | none | its key, used only with a client certificate |

`OTLPSpanExporter`'s `session`, `max_request_size` and `meter_provider`, and its credential provider variables,
have no counterpart. Where fastotel differs from the reference (its own user agent, a header that HTTP cannot
carry raises `ValueError`, `compression` also takes a string) is in
[ADR 0005](https://github.com/troyan-dy/fastotel/blob/master/docs/adr/0005-exporter-configuration.md).

### Delivery

As the OTLP/HTTP specification asks and `OTLPSpanExporter` does: a batch answered with 429, 502, 503 or 504, or
that meets a connection error (refused, dropped, DNS, TLS), is sent again after 1, 2, 4, 8, 16 s, give or take 20%,
or after the answer's `Retry-After`, in at most 6 attempts and only while `timeout` lasts; any other error drops
the batch, and a redirect is not followed and counts as sent.
The spans of a dropped batch and those a collector rejects in a partial success are counted (the counts are public
with #14). Connections are reused across exports. While the collector is down, a batch costs at most `timeout`,
mostly asleep, and the queue keeps memory bounded; exports resume with the next batch once it is back.

An `https://` endpoint is verified through rustls against the OS trust store, so corporate CAs work, or against
`certificate_file`; a TLS file that cannot be read or holds no usable certificate or key raises from the
constructor. Where this differs from the reference (gzip at level 6, TLS files checked at construction, among
others) is in
[ADR 0006](https://github.com/troyan-dy/fastotel/blob/master/docs/adr/0006-production-transport.md).

### Batching

The arguments and variables of `BatchSpanProcessor`, with its defaults, parsing and checks: an argument overrides
its variable, a variable that is not an integer gives the default and an error log on the `fastotel` logger, and a
value out of range raises `ValueError` from the constructor.

| Argument | Variable | Default | What it does |
| --- | --- | --- | --- |
| `max_queue_size` | `OTEL_BSP_MAX_QUEUE_SIZE` | 2048 | spans waiting for export; a span ended while the queue is full is dropped and counted (the count is public with #14), and `on_end` never blocks |
| `max_export_batch_size` | `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | 512 | a batch leaves as soon as it is full; at most `max_queue_size` |
| `schedule_delay_millis` | `OTEL_BSP_SCHEDULE_DELAY` | 5000 | otherwise, what is queued leaves this long after the previous export |
| `export_timeout_millis` | `OTEL_BSP_EXPORT_TIMEOUT` | 30000 | how long `shutdown()` waits for the last export |

Where this differs from `BatchSpanProcessor` (a full queue drops the newest span rather than the oldest, among
others) is in [ADR 0004](https://github.com/troyan-dy/fastotel/blob/master/docs/adr/0004-batching-and-the-queue.md).

### Flush, shutdown and exit

`force_flush(timeout_millis=30000)` exports every span ended before the call and returns False when that takes
longer than the timeout (None waits `export_timeout_millis`), and after `shutdown()`, as `BatchSpanProcessor` does.
`shutdown()` exports what is queued, waiting up to `export_timeout_millis`, stops the worker and is idempotent;
`on_end` after it does nothing but count the span as dropped.

`TracerProvider` shuts its processors down at exit unless created with `shutdown_on_exit=False`; fastotel adds no
exit handler of its own, as `BatchSpanProcessor` does not. Its worker is a native thread that the interpreter does
not wait for, so the process never waits for it beyond that shutdown. Spans still queued when the process ends:

| How the process ends | Spans still queued |
| --- | --- |
| normally, `sys.exit` in the main thread, or an uncaught exception, with the provider's exit handler (the default) | exported, waiting up to `export_timeout_millis` for the collector |
| the same with `TracerProvider(shutdown_on_exit=False)` and no `shutdown()` | lost; exit does not wait |
| `sys.exit` in another thread | that thread ends; the process goes on and ends as above |
| `os._exit`, a fatal signal, `SIGKILL` | lost: no exit handler runs |

Daemon threads still ending spans during and after that shutdown are safe: their spans are dropped and counted. A
Rust panic never reaches the application nor aborts the interpreter: it is caught, counted and logged on the
`fastotel` logger, losing the span or the batch at hand. Calls from many threads, on free-threaded builds too,
export each span once or count it as dropped. Details and where this differs from `BatchSpanProcessor` are in
[ADR 0007](https://github.com/troyan-dy/fastotel/blob/master/docs/adr/0007-flush-shutdown-and-exit.md).

## Development

Needs [uv](https://docs.astral.sh/uv/) and a Rust toolchain ([rustup](https://rustup.rs/)).

```bash
make install   # build the extension, install the dev dependencies
make test      # run the tests, those of the Rust export pipeline (crates/export) too
make lint      # ruff, mypy, cargo fmt, clippy
make bench     # what the stock SDK costs an application, on a GIL and a free-threaded build (~30 min)
```

Every change that reaches `master` is a release: a pull request bumps the version in `pyproject.toml`
(`uv version --bump patch`) and adds its section to `CHANGELOG.md`.

## License

[MIT](https://github.com/troyan-dy/fastotel/blob/master/LICENSE)
