#!/bin/bash

MODULE="$1"
shift 1;
ARGS=$@

export APP_DATADIR=/tmp/faust
python -m "$MODULE" worker -l info $@
