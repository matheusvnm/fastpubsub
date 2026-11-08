set working-directory := "../fastpubsub-core"

wheels_dir := "target/wheels"
stubtest_allowlist := "stubtest-allowlist.txt"

[doc("Execute all core checks (lint, tests and dependency audit)")]
[group("lint")]
@check: lint test audit

[doc("Format the core code with rustfmt")]
[group("lint")]
@format:
    just _start_msg "Formatting the core code"
    cargo fmt --all

[doc("Check the core code with rustfmt, clippy and rustdoc")]
[group("lint")]
@lint:
    just _start_msg "Checking the core formatting"
    cargo fmt --all --check

    just _start_msg "Checking the core lints"
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

    just _start_msg "Checking the core documentation"
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items --locked

[doc("Check the core dependencies for advisories, licenses and unused crates")]
[group("lint")]
@audit:
    just _start_msg "Checking the core dependencies"
    cargo deny check
    cargo machete

[doc("Run the core tests")]
[group("tests")]
@test:
    just _start_msg "Running the core tests"
    cargo nextest run --workspace --all-features --locked --no-tests=pass
    cargo test --doc --workspace --locked

[doc("Check that the core stub matches the compiled module")]
[group("tests")]
@stubtest: build
    just _start_msg "Checking the core stub"
    uv run --active --frozen --directory .. python -m mypy.stubtest fastpubsub_core --allowlist fastpubsub-core/{{ stubtest_allowlist }}

[doc("Build the core into .venv with uv sync, and into .venv-3.14t and .venv-3.15t when they exist")]
[group("build")]
@build:
    just _start_msg "Building the core"
    cd .. && uv sync --all-groups --frozen
    for version in 3.14t 3.15t; do \
        if [ -d "../.venv-$version" ]; then \
            uvx maturin build --release --no-default-features \
                -i "../.venv-$version/bin/python" --out {{ wheels_dir }}; \
            tag="cp$(echo $version | tr -d '.t')"; \
            uv pip install --python "../.venv-$version" --reinstall \
                {{ wheels_dir }}/fastpubsub_core-*-$tag-${tag}t-*.whl; \
        fi; \
    done

[doc("Build the core in the container and install it in the container environment")]
[group("build")]
@build-container:
    just dev up
    just _start_msg "Building the core in the container"
    docker compose exec fastpubsub uv sync --group dev --frozen
