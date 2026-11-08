set dotenv-load

# PROJECT SETTINGS

pubsub_emulator_port := env("PUBSUB_EMULATOR_PORT", "8085")
pubsub_emulator_host := "localhost:" + pubsub_emulator_port

# DIRECTORIES

pre_commit_hook_path := ".git/hooks/pre-commit"

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

# ENVIRONMENT VAR EXPORTS

export PUBSUB_EMULATOR_HOST := pubsub_emulator_host
export PYTHONPATH := "docs/snippets:${PYTHONPATH}"

# MODULES

[doc("Rust commands for the compiled core in fastpubsub-core/")]
mod core "just/core.just"

[doc("Python commands for the fastpubsub package")]
mod public "just/public.just"

[doc("Development commands: infrastructure, benchmarks, setup, git and docs")]
mod dev "just/dev.just"

[doc("All commands information")]
@default:
    just --list --unsorted --list-submodules --list-heading $'FastPubSub commands…\n'

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
    -[ ! -f {{ pre_commit_hook_path }} ] && just _start_msg "FastPubSub: initializing pre-commit" && just dev init
