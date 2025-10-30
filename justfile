
export SCRIPTS_DIR := "./scripts"
export PUBSUB_EMULATOR_HOST := "localhost:8085"

receipts:
    just --list

test test_target=".":
    @bash $SCRIPTS_DIR/tests.sh {{test_target}}

install python_version="3.13":
    @bash $SCRIPTS_DIR/setup.sh
    @bash $SCRIPTS_DIR/install-python.sh {{python_version}}
    @bash $SCRIPTS_DIR/install.sh

lint:
    @bash $SCRIPTS_DIR/lint.sh

format:
    @bash $SCRIPTS_DIR/format.sh

coverage:
    @bash $SCRIPTS_DIR/test-cov.sh

clean:
    @rm -rf .cov htmlcov/ dist/ fastpubsub.egg-info/

execute-pr-test:
   @bash $SCRIPTS_DIR/test-pipeline.sh -W .github/workflows/pr_tests.yaml

execute-release-pypi:
   @bash $SCRIPTS_DIR/test-pipeline.sh -W .github/workflows/release_pypi.yaml
