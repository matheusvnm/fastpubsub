#!/bin/bash

set -e
set -x

# TODO: Adicionar examples
ruff format fastpubsub tests
ruff check fastpubsub tests --select I --fix
ruff check fastpubsub tests --fix
