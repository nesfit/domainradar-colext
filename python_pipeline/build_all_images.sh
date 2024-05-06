#!/bin/bash

units=( "collector" "collector" "collector" "extractor" "classifier" )
modules=( "collector.rdap_ip" "collector.rdap_dn" "collector.rtt" "extractor" "classifier_unit" )

for i in "${!units[@]}"; do
  docker build --target production -t domrad-python/${modules[i]} --build-arg TARGET_UNIT=${units[i]} \
    --build-arg TARGET_MODULE=${modules[i]} .
done
