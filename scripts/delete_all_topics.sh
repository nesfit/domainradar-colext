#!/bin/bash

if [[ $1 == "-h" ]] || [[ $1 == "--help" ]]; then
  echo "$0 [bootstrap server (localhost:9092)]"
  exit 0
fi

BOOTSTRAP=${1:-localhost:9092}

echo Removing all topics
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | xargs -L1 ./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --delete --topic

echo Topics after:
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
