#!/bin/bash

if [[ $1 == "-h" ]] || [[ $1 == "--help" ]]; then
  echo "$0 [bootstrap server (localhost:9092)]"
  echo "    [partitioning of the 'to process' topics (4)] [partitioning of the 'processed' topics (4)]"
  exit 0
fi

BOOTSTRAP=${1:-localhost:9092}
TO_PROCESS_PARTITIONS=${2:-4}
PROCSSED_PARTITIONS=${3:-4}

echo Topics before:
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
echo "-------"

echo Creating to process topics
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic to_process_zone --partitions $TO_PROCESS_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic to_process_DNS --partitions $TO_PROCESS_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic to_process_RDAP_DN --partitions $TO_PROCESS_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic to_process_IP --partitions $TO_PROCESS_PARTITIONS

./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic processed_zone --partitions $PROCSSED_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic processed_DNS --partitions $PROCSSED_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic processed_RDAP_DN --partitions $PROCSSED_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic collected_IP_data --partitions $PROCSSED_PARTITIONS
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --topic merged_IP_data --partitions $PROCSSED_PARTITIONS
echo "-------"

echo Topics after:
./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | xargs -L1 ./kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --describe --topic