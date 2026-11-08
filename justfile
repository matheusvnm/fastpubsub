set dotenv-load

# PROJECT SETTINGS

project_name := "fastpubsub"
python_version := "3.12"
pubsub_emulator_port := env("PUBSUB_EMULATOR_PORT", "8085")
pubsub_emulator_host := "localhost:" + pubsub_emulator_port
docs_port := env("DOCS_PORT", "8001")

# DIRECTORIES

worktrees_dir := ".worktrees"

pre_commit_hook_path := ".git/hooks/pre-commit"
docs_dir := "docs/"

# COLORS

black := `tput -Txterm setaf 0`
red := `tput -Txterm setaf 1`
green := `tput -Txterm setaf 2`
yellow := `tput -Txterm setaf 3`
blue := `tput -Txterm setaf 4`
magenta := `tput -Txterm setaf 5`
cyan := `tput -Txterm setaf 6`
white := `tput -Txterm setaf 7`
bold := `tput -Txterm bold`
reset := `tput -Txterm sgr0`

# COMMAND ALIASES

run_command := "uv run --active --frozen"
run_test_command := "docker compose exec fastpubsub python -m"
benchmark_cpus := "2"

# ENVIRONMENT VAR EXPORTS

export PUBSUB_EMULATOR_HOST := pubsub_emulator_host
export PYTHONPATH := "docs/snippets:${PYTHONPATH}"

# MODULES

[doc("Rust commands for the compiled core in fastpubsub-core/")]
mod core "just/core.just"

[doc("Python commands for the fastpubsub package")]
mod public "just/public.just"

[doc("All commands information")]
@default:
    just --list --unsorted --list-submodules --list-heading $'FastPubSub commands…\n'

[doc("Execute all checks of the Python package and the core")]
[group("lint")]
@check:
    just public check
    just core lint

# ----------------------------------------------------------------------------
# Testing Commands
# ----------------------------------------------------------------------------

[doc("Run CI/CD pipeline locally using act")]
[group("tests")]
@test-pipeline +args:
    just _start_msg "Executing the workflow with act"
    act --secret-file .secrets --var-file .vars {{ args }}

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
@up *args:
    just _start_msg "Starting containers infrastructure"
    docker compose up -d --wait {{ args }}
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
# Benchmark Commands
# ----------------------------------------------------------------------------

[doc("Run benchmarks with fixed CPU limits. Example: just bench --scenario all")]
[group("bench")]
@bench *args:
    just _start_msg "Starting containers with {{ benchmark_cpus }} CPUs each"
    BENCHMARK_CPUS={{ benchmark_cpus }} docker compose up -d --wait
    {{ run_test_command }} benchmarks.bench \
        --git-commit "$(git rev-parse --short HEAD)" {{ args }}
    docker compose down

[doc("Run the FastPubSub cases with the code of a git ref. Example: just bench-ref main --scenario echo")]
[group("bench")]
@bench-ref ref *args:
    just _start_msg "Checking out {{ ref }} in {{ worktrees_dir }}/bench-{{ ref }}"
    git worktree add --force --detach "{{ worktrees_dir }}/bench-{{ ref }}" "{{ ref }}"
    just bench --case fastpubsub-async --label "{{ ref }}" \
        --fastpubsub-source "{{ worktrees_dir }}/bench-{{ ref }}" \
        --git-commit "$(git rev-parse --short {{ ref }})" {{ args }} \
        || true
    git worktree remove --force "{{ worktrees_dir }}/bench-{{ ref }}"

[doc("Run a quick benchmark: one repeat of 10 seconds")]
[group("bench")]
@bench-quick *args:
    just bench --duration 10 --warmup 5 --repeats 1 {{ args }}

[doc("Show the median and the range of the benchmark results")]
[group("bench")]
@bench-results *args:
    just _start_msg "Benchmark Results"
    {{ run_command }} python -m benchmarks.report {{ args }}

[doc("Clear benchmark results")]
[group("bench")]
@bench-clean:
    just _start_msg "Clearing benchmark results"
    rm -f benchmarks/results/runs.csv

# ----------------------------------------------------------------------------
# Local Environment Setup Commands
# ----------------------------------------------------------------------------

[doc("Install uv package manager")]
[group('dev')]
[windows]
@setup:
    just _start_msg "Installing uv package manager"
    powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

[doc("Install uv package manager")]
[group('dev')]
[unix]
@setup:
    just _start_msg "Installing uv package manager"
    curl -LsSf https://astral.sh/uv/install.sh | sh

[doc("Initialize the environment with its dependencies")]
[group('dev')]
@init python=python_version:
    just _start_msg "Setting up python {{ python }}"
    uv python install {{ python }}
    uv python pin {{ python }}
    uv sync --all-groups --all-extras

    just _start_msg "Setting up pre-commit hooks"
    uv run pre-commit install --install-hooks


[doc("Initialize the environment with specific groups without extras")]
[group('dev')]
@init-group python=python_version group="dev":
    just _start_msg "Setting up python {{ python }}"
    uv python install {{ python }}
    uv python pin {{ python }}
    uv sync --group {{ group }}

[doc("Cleans the environment from temporary files")]
[group('dev')]
[windows]
@clean:
    just _start_msg "Removing temporary files"
    powershell -c "Remove-Item -Recurse -Force .cov, htmlcov, dist, fastpubsub.egg-info -ErrorAction SilentlyContinue"

[doc("Cleans the environment from temporary files")]
[group('dev')]
[unix]
@clean:
    just _start_msg "Removing temporary files"
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    rm -rf .pytest_cache htmlcov .coverage .cov dist/ fastpubsub.egg-info/ 2>/dev/null || true
    rm -rf site/ docs/.cache .cache/ 2>/dev/null || true
    rm -rf *.log 2>/dev/null || true

# ----------------------------------------------------------------------------
# Git Commands
# ----------------------------------------------------------------------------

[doc("Create a worktree in .worktrees/ with its own container ports")]
[group('git')]
[unix]
worktree-add branch base="main":
    #!/usr/bin/env bash
    set -euo pipefail
    name="{{ replace(branch, '/', '-') }}"
    path="{{ worktrees_dir }}/$name"
    offset=1
    while grep -qsx "PUBSUB_EMULATOR_PORT=$((8085 + offset * 10))" {{ worktrees_dir }}/*/.env; do
        offset=$((offset + 1))
    done
    just _start_msg "Creating worktree $path"
    if git show-ref --verify --quiet "refs/heads/{{ branch }}"; then
        git worktree add "$path" "{{ branch }}"
    else
        git worktree add -b "{{ branch }}" "$path" "{{ base }}"
    fi
    printf '%s\n' \
        "COMPOSE_PROJECT_NAME={{ project_name }}-$name" \
        "PUBSUB_EMULATOR_PORT=$((8085 + offset * 10))" \
        "DOCS_PORT=$((8001 + offset * 10))" > "$path/.env"
    just _start_msg "Installing dependencies in $path"
    (cd "$path" && uv sync --all-groups --all-extras)
    just _green "Worktree ready: $path (emulator port $((8085 + offset * 10)), docs port $((8001 + offset * 10)))"

[doc("Stop the containers of a worktree and remove it")]
[group('git')]
[unix]
worktree-remove branch:
    #!/usr/bin/env bash
    set -euo pipefail
    path="{{ worktrees_dir }}/{{ replace(branch, '/', '-') }}"
    just _start_msg "Removing containers and volumes of $path"
    (cd "$path" && docker compose down --volumes --remove-orphans) \
        || just _warn_msg "Could not remove the containers of $path"
    just _start_msg "Removing worktree $path"
    git worktree remove "$path"

# ----------------------------------------------------------------------------
# Documentation Commands
# ----------------------------------------------------------------------------

[doc("Creates the Zensical static documentation")]
[group('docs')]
@build-docs:
    just _start_msg "Building documentation"
    uv run zensical build --clean -f docs/zensical.toml

[doc("Creates the Zensical static documentation")]
[group('docs')]
@serve-docs:
    just _start_msg "Starting documentation server"
    uv run zensical serve -f {{ docs_dir }}/zensical.toml -a localhost:{{ docs_port }}

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
