#!/bin/bash

set -e
set -x

uv sync --group dev --all-extras
uv run pre-commit install --install-hooks
