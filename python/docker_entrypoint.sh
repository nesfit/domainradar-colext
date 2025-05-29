#!/bin/bash

MODULE="$1"
shift 1;
python -m "$MODULE" $@
