# PROJECT SETTINGS
project_name := "fastpubsub"
python_version := "3.12"
pubsub_emulator_host := "localhost:8085"

# DIRECTORIES
target_dirs := "fastpubsub tests examples"
lint_dirs := "fastpubsub examples"
lint_extra_dirs := "fastpubsub examples tests"
pre_commit_hook_path := ".git/hooks/pre-commit"

# COLORS
black   := `tput -Txterm setaf 0`
red     := `tput -Txterm setaf 1`
green   := `tput -Txterm setaf 2`
yellow  := `tput -Txterm setaf 3`
blue    := `tput -Txterm setaf 4`
magenta := `tput -Txterm setaf 5`
cyan    := `tput -Txterm setaf 6`
white   := `tput -Txterm setaf 7`
bold    := `tput -Txterm bold`
reset   := `tput -Txterm sgr0`

# COMMAND ALIASES
run_command := "uv run --active --frozen"
run_test_command := "docker compose exec fastpubsub python -m"


# ENVIRONMENT VAR EXPORTS
export PUBSUB_EMULATOR_HOST := pubsub_emulator_host

[doc("All commands information")]
@default:
  just --list --unsorted --list-heading $'FastPubSub commands…\n'

# ----------------------------------------------------------------------------
# Testing Commands
# ----------------------------------------------------------------------------

[doc("Run CI/CD pipeline locally using act")]
[group("tests")]
@test-pipeline +args:
    just _start_msg "Executing the workflow with act"
    act --secret-file .secrets --var-file .vars {{ args }}

[doc("Run unit tests (fast, no emulator)")]
[group("tests")]
@test-unit *args: up && down
    just _start_msg "Running unit tests"
    {{ run_test_command }} pytest -m "not connected and not slow" -n auto --tb=short {{ args }}

[doc("Run integration tests (requires emulator)")]
[group("tests")]
@test-integration *args: up && down
    just _start_msg "Running integration tests"
    {{ run_test_command }} pytest -m "connected" -n auto --tb=short --maxfail=5 {{ args }}

[doc("Run all tests in Docker")]
[group("tests")]
@test-all *args: up && down
    just _start_msg "Running all tests"
    {{ run_test_command }} pytest -n auto --tb=short {{ args }}

[doc("Run tests with coverage in Docker")]
[group("tests")]
@test-cov *args: up && down
    just _start_msg "Running coverage report"
    {{ run_test_command }} pytest \
        -n auto \
        --cov=fastpubsub \
        --cov-report=term-missing:skip-covered \
        --cov-report=html \
        --cov-fail-under=80 \
        {{ args }}

[doc("Run tests matching a keyword in Docker")]
[group("tests")]
@test-k keyword *args: up && down
    just _start_msg "Running tests matching '{{ keyword }}'"
    {{ run_test_command }} pytest -k "{{ keyword }}" -v {{ args }}


# ----------------------------------------------------------------------------
# Linting Tools Commands
# ----------------------------------------------------------------------------
[doc("Execute all checks (lint, security and static analysis)")]
[group("lint")]
@check: typo lint securitize

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
    {{ run_command }} mypy {{lint_dirs}}

    just _start_msg "Checking linting rules"
    {{ run_command }} ruff check {{ lint_extra_dirs }}

    just _start_msg "Checking formatting rules"
    {{ run_command }} ruff format {{ lint_extra_dirs }} --check

[doc("Checks misspellings with codespell")]
[group("lint")]
@typo:
    just _start_msg "Checking misspellings on words"
    {{ run_command }} codespell fastpubsub

[doc("Executes security analysis on code")]
[group("lint")]
@securitize:
  just _start_msg "Checking for vulnerabilities"
  {{ run_command }} bandit -c pyproject.toml -r {{ project_name }}

# ----------------------------------------------------------------------------
# Infra Commands
# ----------------------------------------------------------------------------

[doc("Build test Docker images")]
[group("infra")]
@build *args:
    just _start_msg "Building containers images"
    docker compose build {{ args }}

[doc("Run all containers")]
[group("infra")]
@up:
    just _start_msg "Starting containers infrastructure"
    docker compose up -d --wait 2> /dev/null
    just _start_msg "Infrastructure ready!"

[doc("Execute top command on executing containers")]
[group("infra")]
@top:
    just _start_msg "Checking the containers resources."
    docker compose top

[doc("Execute ps command on executing containers")]
[group("infra")]
@ps:
    just _start_msg "Checking the containers running"
    docker compose ps

[doc("Stop all containers")]
[group("infra")]
@stop:
    just _start_msg "Stopping the containers"
    docker compose stop

[doc("Down all containers")]
[group("infra")]
@down:
    just _start_msg "Stopping containers infrastructure"
    docker compose down

[doc("Purge containers infrastructure and volumes")]
[group("infra")]
@purge:
    just _start_msg "Purging all containers and volumes"
    docker compose down --volumes --remove-orphans

[doc("Open shell in container")]
[group("infra")]
@shell: up
    docker compose exec fastpubsub bash


# ----------------------------------------------------------------------------
# Local Environment Setup Commands
# ----------------------------------------------------------------------------

[doc("Install uv package manager")]
[windows]
[group('dev')]
@setup:
    just _start_msg "Installing uv package manager"
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

[doc("Install uv package manager")]
[unix]
[group('dev')]
@setup:
    just _start_msg "Installing uv package manager"
    curl -LsSf https://astral.sh/uv/install.sh | sh


[doc("Initialize the environment with its dependencies")]
[group('dev')]
@init python=python_version:
    just _start_msg "Setting up python {{python}}"
    uv python install {{python}}
    uv python pin {{python}}
    uv sync --all-groups --all-extras

    just _start_msg "Setting up pre-commit hooks"
    uv run pre-commit install --install-hooks

[doc("Cleans the environment from temporary files")]
[windows]
[group('dev')]
@clean:
    just _start_msg "Removing temporary files"
    powershell -c "Remove-Item -Recurse -Force .cov, htmlcov, dist, fastpubsub.egg-info -ErrorAction SilentlyContinue"

[doc("Cleans the environment from temporary files")]
[unix]
[group('dev')]
@clean:
    just _start_msg "Removing temporary files"
    rm -rf .cov htmlcov/ dist/ fastpubsub.egg-info/


# ----------------------------------------------------------------------------
# Private Commands
# ----------------------------------------------------------------------------

[private]
@_red message:
    echo "{{ red }}{{ message }}{{ reset }}"

[private]
@_green message:
    echo "{{ green }}{{ message }}{{ reset }}"

[private]
@_yellow message:
    echo "{{ yellow }}{{ message }}{{ reset }}"

[private]
@_blue message:
    echo "{{ blue }}{{ message }}{{ reset }}"

[private]
@_start_msg msg:
    just _green "{{ msg }}..."

[private]
@_warn_msg msg:
    just _red "{{ msg }}..."

[private]
@_pre:
    # To be run before normal development commands to make sure the environment is setup correctly.
    # - .env file must exist (Manual remediation)
    # - {{ pre_commit_hook_path }} must exist (Automatic remediation)
    -[ ! -f {{ pre_commit_hook_path }} ] && just _start_msg "FastPubSub: initializing pre-commit" && just init-pre-commit
