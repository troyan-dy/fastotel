# fastotel

[![PyPI](https://img.shields.io/pypi/v/fastotel)](https://pypi.org/project/fastotel/)
[![Python](https://img.shields.io/pypi/pyversions/fastotel)](https://pypi.org/project/fastotel/)
[![CI](https://github.com/troyan-dy/fastotel/actions/workflows/ci.yml/badge.svg)](https://github.com/troyan-dy/fastotel/actions/workflows/ci.yml)
[![License](https://img.shields.io/pypi/l/fastotel)](https://github.com/troyan-dy/fastotel/blob/master/LICENSE)

A Rust-backed drop-in for the OpenTelemetry Python SDK that takes tracing overhead off the request path.

> **Status: pre-alpha.** The package is a skeleton: it installs and imports, and nothing replaces the SDK yet.
> The plan is in [#1](https://github.com/troyan-dy/fastotel/issues/1).

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

## Development

Needs [uv](https://docs.astral.sh/uv/) and a Rust toolchain ([rustup](https://rustup.rs/)).

```bash
make install   # build the extension, install the dev dependencies
make test      # run the tests
make lint      # ruff, mypy, cargo fmt, clippy
make bench     # what the stock SDK costs an application, on a GIL and a free-threaded build (~30 min)
```

Every change that reaches `master` is a release: a pull request bumps the version in `pyproject.toml`
(`uv version --bump patch`) and adds its section to `CHANGELOG.md`.

## License

[MIT](https://github.com/troyan-dy/fastotel/blob/master/LICENSE)
