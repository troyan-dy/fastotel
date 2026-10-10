.DEFAULT_GOAL := help

PY_VERSIONS := 3.11 3.12 3.13 3.14 3.14t 3.15 3.15t
# make bench measures on a build with the GIL and on a free-threaded one
BENCH_PYTHONS := 3.14 3.14t
# The build script of PyO3 checks the interpreter against abi3-py311; the system python3 may be older
export PYO3_PYTHON ?= $(CURDIR)/.venv/bin/python

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: install
install: ## Build the extension and install it with the dev dependencies
	uv sync --locked

.PHONY: lint
lint: ## Run ruff, mypy, cargo fmt and clippy
	uv run ruff check .
	uv run ruff format --check .
	uv run --group bench mypy
	cargo fmt --check
	cargo clippy --locked --all-targets -- -D warnings

.PHONY: format
format: ## Autofix lint issues and format the code
	uv run ruff check --fix .
	uv run ruff format .
	cargo fmt

.PHONY: test
test: ## Run tests, those of the benchmark harness too
	uv run pytest
	uv run --group bench pytest bench

.PHONY: test-all
test-all: ## Run tests on every supported Python version, free-threaded and pre-release ones too
	@for v in $(PY_VERSIONS); do \
		echo "==> Python $$v"; \
		uv run --isolated --python $$v pytest || exit 1; \
	done

.PHONY: bench
bench: ## Measure what the SDK's span export costs (bench/), about 30 minutes; pyperf options go in BENCH_ARGS
	@rm -rf .bench && mkdir .bench
	@for v in $(BENCH_PYTHONS); do \
		echo "==> Python $$v"; \
		uv run --isolated --python $$v --group bench python -m bench.run --quiet --output .bench/$$v.json $(BENCH_ARGS) || exit 1; \
	done
	@uv run --group bench python -m bench.table $(foreach v,$(BENCH_PYTHONS),.bench/$(v).json)

.PHONY: check-version
check-version: ## Check that the version is bumped against origin/master, as CI does on a pull request
	uv run --no-project python scripts/version.py check origin/master
