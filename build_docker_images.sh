#!/bin/bash

# Usage: $0 [-q|-qb] [java|python|all] [additional docker build options]"
#   -q: quiet mode, suppresses all output"
#   -qb: quiet build mode, suppresses Docker build output"
#
# Builds Docker images for the Java and Python pipeline components.
# If no component type is specified, it builds all components.

TAG_PREFIX="domrad"

py_packages=("collector" "collector" "collector" "collector" "extractor" "classifier")
py_modules=("collectors.zone" "collectors.rdap_ip" "collectors.rdap_dn" "collectors.rtt" "extractor" "classifier_unit")
py_tags=("zone" "rdap-ip" "rdap-dn" "rtt" "extractor" "classifier")

STREAMS_TAG="java-streams"
PARCON_TAG="java-standalone"
CONNECT_TAG="kafka-connect"

OUT_BUILD=/dev/stdout
OUT_MSG=/dev/stdout

build_java() {
  echo ">>> Building images for Java-based pipeline components <<<" >"$OUT_MSG"
  cd java_pipeline || return 1

  echo "  > Building Kafka Streams components <  "
  echo "    > Tag: '$TAG_PREFIX/$STREAMS_TAG' < "

  docker build -f components.Dockerfile --build-arg TARGET_PKG=streams-components --target runtime-streams -t "$TAG_PREFIX/$STREAMS_TAG" "$@" . 2>"$OUT_BUILD"

  echo "  > Building Parallel Consumer components <  " >"$OUT_MSG"
  echo "    > Tag: '$TAG_PREFIX/$PARCON_TAG' < " >"$OUT_MSG"

  docker build -f components.Dockerfile --build-arg TARGET_PKG=standalone-collectors --target runtime-standalone -t "$TAG_PREFIX/$PARCON_TAG" "$@" . 2>"$OUT_BUILD"

  echo "  > Building Kafka Connect base image <  " >"$OUT_MSG"
  echo "    > Tag: '$TAG_PREFIX/$CONNECT_TAG' < " >"$OUT_MSG"

  docker build -f connect.Dockerfile --target runtime-connect -t "$TAG_PREFIX/$CONNECT_TAG" "$@" . 2>"$OUT_BUILD"

  cd ..
}

build_python() {
  echo ">>> Building images for Python-based pipeline components <<<" >"$OUT_MSG"
  cd python_pipeline || return 1

  for i in "${!py_packages[@]}"; do
    echo "  > Building ${py_modules[i]} <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/${py_tags[i]}' < " >"$OUT_MSG"

    docker build --target production -t "$TAG_PREFIX/${py_tags[i]}" --build-arg "TARGET_UNIT=${py_packages[i]}" \
      --build-arg "TARGET_MODULE=${py_modules[i]}" "$@" . 2>"$OUT_BUILD"
  done

  cd ..
}

if [ "$1" = "help" ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  echo "Usage: $0 [-q|-qb] [java|python|all] [additional docker build options]"
  echo "  -q: quiet mode, suppresses all output"
  echo "  -qb: quiet build mode, suppresses Docker build output"
  echo ""
  echo "Builds Docker images for the Java and Python pipeline components."
  echo "If no component type is specified, it builds all components."
  exit 0
fi

if [ "$1" = "-q" ]; then
  OUT_BUILD=/dev/null
  OUT_MSG=/dev/null
  shift 1
elif [ "$1" = "-qb" ]; then
  OUT_BUILD=/dev/null
  shift 1
fi

if [ "$1" = "java" ] || [ "$1" = "all" ]; then
  shift 1
  build_java "$@"
elif [ "$1" = "python" ] || [ "$1" = "all" ]; then
  shift 1
  build_python "$@"
else
  if [ "$1" = "all" ]; then
    shift 1
  fi

  build_java "$@"
  build_python "$@"
fi
