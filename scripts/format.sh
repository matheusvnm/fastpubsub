#!/bin/bash

set -e
set -x

ruff format fastpubsub
ruff check fastpubsub --select I --fix
ruff check fastpubsub --fix


ruff format examples
ruff check examples --select I --fix
ruff check examples --fix
