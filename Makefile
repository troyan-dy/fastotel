.DEFAULT_GOAL := help

PY_VERSIONS := 3.11 3.12 3.13 3.14
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
	uv run mypy
	cargo fmt --check
	cargo clippy --locked --all-targets -- -D warnings

.PHONY: format
format: ## Autofix lint issues and format the code
	uv run ruff check --fix .
	uv run ruff format .
	cargo fmt

.PHONY: test
test: ## Run tests
	uv run pytest

.PHONY: test-all
test-all: ## Run tests on every supported Python version
	@for v in $(PY_VERSIONS); do \
		echo "==> Python $$v"; \
		uv run --isolated --python $$v pytest || exit 1; \
	done

.PHONY: check-version
check-version: ## Check that the version is bumped against origin/master, as CI does on a pull request
	uv run --no-project python scripts/version.py check origin/master
