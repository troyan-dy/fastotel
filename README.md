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

Wheels are built for CPython 3.11 and newer on Linux (glibc and musl), macOS and Windows, x86_64 and arm64.

## Development

Needs [uv](https://docs.astral.sh/uv/) and a Rust toolchain ([rustup](https://rustup.rs/)).

```bash
make install   # build the extension, install the dev dependencies
make test      # run the tests
make lint      # ruff, mypy, cargo fmt, clippy
```

Every change that reaches `master` is a release: a pull request bumps the version in `pyproject.toml`
(`uv version --bump patch`) and adds its section to `CHANGELOG.md`.

## License

[MIT](https://github.com/troyan-dy/fastotel/blob/master/LICENSE)
