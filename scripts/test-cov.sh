#!/bin/bash

set -e
set -x

coverage report
coverage html
