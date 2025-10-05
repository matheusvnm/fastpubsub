#!/bin/bash

set -e
set -x

TARGET_DIRECTORIES="fastpubsub tests examples/*"

ruff format $TARGET_DIRECTORIES
ruff check $TARGET_DIRECTORIES --select I --fix
ruff check $TARGET_DIRECTORIES --fix
