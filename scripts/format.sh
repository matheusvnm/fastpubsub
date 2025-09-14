#!/bin/bash

set -e
set -x

ruff format fastpubsub
ruff check fastpubsub --select I --fix
ruff check fastpubsub --fix
