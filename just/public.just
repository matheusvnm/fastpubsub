set dotenv-load
set working-directory := ".."

typecheck_dirs := "fastpubsub benchmarks"
target_dirs := "fastpubsub tests docs/snippets benchmarks"
lint_dirs := "fastpubsub tests docs/snippets benchmarks"
run_command := "uv run --active --frozen"
run_test_command := "docker compose exec fastpubsub python -m"

export PUBSUB_EMULATOR_HOST := "localhost:" + env("PUBSUB_EMULATOR_PORT", "8085")
export PYTHONPATH := "docs/snippets:${PYTHONPATH}"

[doc("Run unit tests (fast, no emulator)")]
[group("tests")]
@test-unit *args:
    just up "--no-deps fastpubsub"
    just _start_msg "Running unit tests"
    {{ run_test_command }} pytest -m "not (connected or slow or docs)" -n auto --tb=short {{ args }}
    just down

[doc("Run integration tests (requires emulator)")]
[group("tests")]
@test-integration *args:
    just up
    just _start_msg "Running integration tests"
    {{ run_test_command }} pytest -m "connected" -n auto --tb=short --maxfail=5 {{ args }}
    just down

[doc("Run doc snippets tests (fast, no emulator)")]
[group("tests")]
@test-docs *args:
    just up "--no-deps fastpubsub"
    just _start_msg "Running the tests for doc snippets"
    {{ run_test_command }} pytest -m "docs" -n auto --tb=short {{ args }}
    just down

[doc("Run all tests in Docker")]
[group("tests")]
@test-all *args:
    just up
    just _start_msg "Running all tests"
    {{ run_test_command }} pytest -n auto --tb=short {{ args }}
    just down

[doc("Run tests with coverage in Docker")]
[group("tests")]
@test-cov *args:
    just up
    just _start_msg "Running coverage report"
    {{ run_test_command }} pytest \
        -n auto \
        --cov=fastpubsub \
        --cov-report=term-missing:skip-covered \
        --cov-report=html \
        --cov-fail-under=80 \
        {{ args }}
    just down

[doc("Run tests matching a keyword in Docker")]
[group("tests")]
@test-k keyword *args:
    just up
    just _start_msg "Running tests matching '{{ keyword }}'"
    {{ run_test_command }} pytest -k "{{ keyword }}" -v {{ args }}
    just down

[doc("Execute all Python checks (lint, docstrings, typos and security)")]
[group("lint")]
@check: typo lint docstrings securitize

[doc("Formatting and sorting with ruff")]
[group("lint")]
@format target=target_dirs:
    just _start_msg "Applying formatting rules"
    {{ run_command }} ruff format {{ target }}

    just _start_msg "Applying import sorting rules"
    {{ run_command }} ruff check {{ target }} --select I --fix

    just _start_msg "Applying linting rules"
    {{ run_command }} ruff check {{ target }} --fix

[doc("Checks linting rules with mypy and ruff")]
[group("lint")]
@lint:
    just _start_msg "Checking typing rules"
    {{ run_command }} mypy {{ typecheck_dirs }}

    just _start_msg "Checking linting rules"
    {{ run_command }} ruff check {{ lint_dirs }}

    just _start_msg "Checking formatting rules"
    {{ run_command }} ruff format {{ lint_dirs }} --check

[doc("Checks that each module, class and function has a docstring")]
[group("lint")]
@docstrings:
    just _start_msg "Checking docstrings"
    {{ run_command }} docstr-coverage fastpubsub --exclude "__about__" --verbose 1

[doc("Checks misspellings with codespell")]
[group("lint")]
@typo:
    just _start_msg "Checking misspellings on words"
    {{ run_command }} codespell fastpubsub

[doc("Executes security analysis on code")]
[group("lint")]
@securitize:
    just _start_msg "Checking for vulnerabilities"
    {{ run_command }} bandit -c pyproject.toml -r fastpubsub
