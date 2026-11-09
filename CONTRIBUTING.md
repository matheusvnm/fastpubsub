# Contributing to FastPubSub

This guide explains how to set up the project, where the code is, and which checks a change must pass.

## Requirements

- [uv](https://docs.astral.sh/uv/) for Python, the dependencies and the builds.
- [just](https://just.systems) for the project recipes.
- [Docker](https://docs.docker.com/get-docker/) for the Pub/Sub emulator and the test container.
- [rustup](https://rustup.rs) for the Rust toolchain. `fastpubsub-core/rust-toolchain.toml` pins the version, and rustup installs it with `rustfmt` and `clippy` at the first use.
- For the core checks: `cargo install --locked cargo-nextest cargo-deny cargo-machete`.

## Setup

```bash
just dev init
```

The recipe installs Python 3.12, syncs all dependency groups and installs the pre-commit hooks for the commit, the commit message and the push.

`uv sync` builds `fastpubsub-core` with maturin and installs it in `.venv` as a normal wheel. After a change to a Rust file, a `Cargo.toml` or the stub, the next `uv sync` or `uv run` builds it again. These local builds use the Cargo profile `local`, without link-time optimization, so they are fast. The release wheels use the profile `release`.

## Layout

```
fastpubsub/                 the Python package (pure Python)
  _internal/                internal code, grouped by feature
    clients.py              PubSubClient, PubSubClientFactory
    connection.py           the emulator endpoint and the resource paths
    event_loop_bridge.py    one core Bridge for each event loop
    delivery/               the subscriber task and the task manager
fastpubsub-core/            the compiled core (Rust, PyO3, maturin)
  fastpubsub_core.pyi       the typed stub of the compiled module
  crates/messaging/         message types that do not depend on a broker
  crates/google-pubsub/     the Google Pub/Sub adapter, without PyO3
  crates/bindings/          the PyO3 bindings, one file for each class
  tests/                    pytest tests for the bindings
scripts/                    the just modules core, public and dev
```

Directories in `fastpubsub/` group features. Do not name a module or a directory after a language.

A new broker, for example Apache Pulsar, goes in a new adapter crate next to `crates/google-pubsub`. It converts its messages to the types of `crates/messaging`.

## Recipes

The recipes are in three modules:

| Module | Use | Examples |
|--------|-----|----------|
| `just core` | The Rust core | `format`, `lint`, `test`, `audit`, `stubtest`, `build`, `check` |
| `just public` | The Python package | `format`, `lint`, `docstrings`, `typo`, `securitize`, `test-unit`, `test-all`, `check` |
| `just dev` | Infrastructure, benchmarks, setup, git and docs | `up`, `down`, `bench`, `init`, `serve-docs`, `check` |

Run `just` to see all recipes. `just dev check` runs the Python checks and the core lint.

## Tests

- The Python tests run in Docker with the emulator: `just public test-all`.
- The core tests run with `just core test`. The integration tests of the adapter crate run only when `PUBSUB_EMULATOR_HOST` is set. Start the emulator with `just dev up` first.
- `just core stubtest` checks that `fastpubsub_core.pyi` matches the compiled module. Change the stub in the same commit as the Rust code.
- For free-threaded Python, make a separate environment:

  ```bash
  UV_PROJECT_ENVIRONMENT=.venv-3.14t uv sync --python 3.14t --no-default-groups --group test --group bench
  ```

  Then run `PYTHON_GIL=0 .venv-3.14t/bin/python -m pytest`.

Some subscription settings behave differently on the emulator and on the Pub/Sub service. The emulator does not apply the retry backoff or exactly-once delivery. The tests for those settings run only against the service.

## Documentation in the code

- Each Python module, class, function and method has a Google-style docstring, private items too. `just public docstrings` checks this.
- Each Rust file starts with a `//!` module doc. Each Rust item has a `///` doc, private items too. A function that returns `Result` has an `# Errors` section. The workspace lints fail the build when a doc is missing.
- The `///` doc of a PyO3 class or method becomes its Python `__doc__`. Write it for a Python reader.
- Do not write code comments. Put the explanation in a docstring, or make the code clear. The pre-commit hooks reject full-line comments in Python and Rust. Tool directives such as `# type: ignore`, `# noqa` and `# pragma: no cover` are allowed.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org), for example `fix(core): ...` or `docs: ...`. The commit message hook checks the format.
