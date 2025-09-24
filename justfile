
export SCRIPTS_DIR := "./scripts"
export PUBSUB_EMULATOR_HOST := "localhost:8085"

default_test_target := "."


receipts:
    just --list


build:
    uv build

test test_target=default_test_target:
    @bash $SCRIPTS_DIR/tests.sh {{test_target}}

lint:
    @bash $SCRIPTS_DIR/lint.sh

format:
    @bash $SCRIPTS_DIR/format.sh

coverage:
    @bash $SCRIPTS_DIR/test-cov.sh

publish:
    uv publish
