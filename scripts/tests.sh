#!/bin/bash

set -e 
set -x


coverage run -m pytest ${@}