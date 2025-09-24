#!/bin/bash

set -e
set -x

# TODO: Adicionar examples/tests
mypy fastpubsub
ruff check fastpubsub
ruff format fastpubsub --check
