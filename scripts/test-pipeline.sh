#!/bin/bash

set -e
set -x

act --secret-file .secrets --var-file .vars ${@}
