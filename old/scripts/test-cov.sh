#!/bin/bash

set -e 
set -x

coverage combine
coverage report
coverage html