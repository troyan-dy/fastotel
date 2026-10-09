# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-09

### Added

- The package skeleton: a PyO3 extension module built with maturin, `fastotel.__version__`, type stubs and
  `py.typed`. Nothing replaces the OpenTelemetry SDK yet; see #1 for the plan.
- `abi3` wheels for CPython 3.11 and newer on Linux (manylinux and musllinux, x86_64 and aarch64), macOS
  (x86_64 and arm64) and Windows (x64 and arm64), plus the sdist.
- Releases: every version that reaches `master` is published to PyPI through Trusted Publishing, tagged and
  given release notes from this file.

[Unreleased]: https://github.com/troyan-dy/fastotel/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/troyan-dy/fastotel/releases/tag/v0.1.0
