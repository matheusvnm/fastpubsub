#!/bin/bash

set -e
set -x

mypy fastpubsub
ruff check fastpubsub
ruff format fastpubsub --check
