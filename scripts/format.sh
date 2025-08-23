#!/bin/bash

set -e
set -x

ruff format starconsumers
ruff check starconsumers --select I --fix
ruff check starconsumers --fix
