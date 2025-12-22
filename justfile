# TODO: [ACT] We need to be able to install that no matter the system
# TODO: [ACT] We also need to warn about needed dependencies (.secrets + .vars)
# TODO: [LINT] We need to add new lint commands to the pipeline
# TODO: [LINT] We need to add new lint commands to pre-commit

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

run_command := "uv run --active --frozen"


export PUBSUB_EMULATOR_HOST := pubsub_emulator_host

[doc("All commands information")]
default:
  @just --list --unsorted --list-heading $'FastPubSub commands…\n'

# ----------------------------------------------------------------------------
# Testing Tools Commands
# ----------------------------------------------------------------------------

[doc("Run tests")]
[group("tests")]
@test +args=".":
    {{ run_command }} coverage run -m pytest {{ args }}

[doc("Run coverage report")]
[group("tests")]
@coverage: test
    {{ run_command }} coverage report
    {{ run_command }} coverage html

[doc("Run CI/CD pipeline locally using act")]
[group("tests")]
@test-pipeline +args:
    just _start_msg "Executing the workflow with act"
    act --secret-file .secrets --var-file .vars {{ args }}


# ----------------------------------------------------------------------------
# Linting Tools Commands
# ----------------------------------------------------------------------------
[doc("Execute all checks (lint, security and static analysis)")]
[group("lint")]
@check: typo lint securitize analyze

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


[doc("Executes static analysis on CI/CD")]
[group("lint")]
@analyze:
  just _start_msg "Performing static analysis on CI/CD pipeline"
  {{ run_command }} zizmor .

# ----------------------------------------------------------------------------
# Infra Commands
# ----------------------------------------------------------------------------

[doc("Run all containers")]
[group("infra")]
@up:
    just _start_msg "Starting the containers"
    docker compose up -d

[doc("Execute top command on executing containers")]
[group("infra")]
@top:
    just _start_msg "Checking the containers resource"
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
    just _start_msg "Shutting down the containers"
    docker compose down

[doc("Down all containers purging the volumes")]
[group("infra")]
@purge:
    just _start_msg "Shutting down the containers and removing volumes"
    docker compose down --volumes

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
@init python=python_version: setup
    just _start_msg "Setting up python {{python_version}}"
    uv python install {{python}}
    uv python pin {{python}}
    uv sync --group dev --all-extras

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
