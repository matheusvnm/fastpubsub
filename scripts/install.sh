#!/bin/bash

set -e
set -x

uv sync --group dev --all-extras
pre-commit install --install-hooks
