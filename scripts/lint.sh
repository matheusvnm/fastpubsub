#!/bin/bash

set -e
set -x

mypy starconsumers
ruff check starconsumers
ruff format starconsumers --check