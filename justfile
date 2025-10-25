
export SCRIPTS_DIR := "./scripts"
export PUBSUB_EMULATOR_HOST := "localhost:8085"

receipts:
    just --list

test test_target=".":
    @bash $SCRIPTS_DIR/tests.sh {{test_target}}

install-python python_version="3.13":
    @bash $SCRIPTS_DIR/install-python.sh {{python_version}}

lint:
    @bash $SCRIPTS_DIR/lint.sh

format:
    @bash $SCRIPTS_DIR/format.sh

coverage:
    @bash $SCRIPTS_DIR/test-cov.sh

test-pipeline-pr-test:
   @bash $SCRIPTS_DIR/test-pipeline.sh -W .github/workflows/pr_tests.yaml

test-pipeline-release-github:
   @bash $SCRIPTS_DIR/test-pipeline.sh -W .github/workflows/release_github.yaml

test-pipeline-release-pypi:
   @bash $SCRIPTS_DIR/test-pipeline.sh -W .github/workflows/release_github.yaml