#!/bin/bash

set -e
set -x

uv python install ${@}
uv python pin ${@}
