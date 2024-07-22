#!/bin/bash

# Usage: ./build_images.sh [-q|-qb] [java|python [<component tag>|all]] [additional docker build options]"
#   -q: quiet mode, suppresses all output"
#   -qb: quiet build mode, suppresses Docker build output"
#
# Builds container images for the Java and Python pipeline components.
# If no component type is specified, builds all components.
# The tag prefix is '$TAG_PREFIX/'.
# You can build a single image by specifying the tag (without the tag prefix).

TAG_PREFIX="domrad"

py_packages=("collector" "collector" "collector" "collector" "collector" "extractor")
py_modules=("collectors.zone" "collectors.dns" "collectors.rdap_ip" "collectors.rdap_dn" "collectors.rtt" "extractor")
py_tags=("zone" "dns" "rdap-ip" "rdap-dn" "rtt" "extractor")

STREAMS_TAG="java-streams"
PARCON_TAG="java-standalone"
CONNECT_TAG="kafka-connect"

STANDALONE_INPUT_TAG="standalone-input"
CONFIG_MANAGER_TAG="config-manager"

OUT_BUILD=/dev/stdout
OUT_MSG=/dev/stdout


build_java() {
  echo ">>> Building images for Java-based pipeline components <<<" >"$OUT_MSG"
  cd java_pipeline || return 1

  local build_streams=1
  local build_parcon=1
  local build_connect=1

  if [ -n "$1" ]; then
    if [ "$1" == "$STREAMS_TAG" ]; then
      build_parcon=0
      build_connect=0
      shift 1
    elif [ "$1" == "$PARCON_TAG" ]; then
      build_streams=0
      build_connect=0
      shift 1
    elif [ "$1" == "$CONNECT_TAG" ]; then
      build_streams=0
      build_parcon=0
      shift 1
    fi
  fi

  if [ "$build_streams" == 1 ]; then
    echo "  > Building Kafka Streams components <  "
    echo "    > Tag: '$TAG_PREFIX/$STREAMS_TAG' < "

    docker build -f streams.Dockerfile -t "$TAG_PREFIX/$STREAMS_TAG" "$@" . 2>"$OUT_BUILD"
  fi

  if [ "$build_parcon" == 1 ]; then
    echo "  > Building Parallel Consumer components <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/$PARCON_TAG' < " >"$OUT_MSG"

    docker build -f standalone.Dockerfile -t "$TAG_PREFIX/$PARCON_TAG" "$@" . 2>"$OUT_BUILD"
  fi

  if [ "$build_connect" == 1 ]; then
    echo "  > Building Kafka Connect base image <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/$CONNECT_TAG' < " >"$OUT_MSG"

    docker build -f connect.Dockerfile --target runtime-connect -t "$TAG_PREFIX/$CONNECT_TAG" "$@" . 2>"$OUT_BUILD"
  fi

  cd ..
}

build_python_standalone_input() {
  echo ">>> Building image for the standalone input controller <<<" >"$OUT_MSG"
  echo "    > Tag: '$TAG_PREFIX/$STANDALONE_INPUT_TAG' < " >"$OUT_MSG"

  docker build --target production -t "$TAG_PREFIX/$STANDALONE_INPUT_TAG" "$@" . 2>"$OUT_BUILD"
}

build_python_config_manager() {
  echo ">>> Building image for the config manager <<<" >"$OUT_MSG"
  echo "    > Tag: '$TAG_PREFIX/$CONFIG_MANAGER_TAG' < " >"$OUT_MSG"

  docker build --target production -t "$TAG_PREFIX/$CONFIG_MANAGER_TAG" "$@" . 2>"$OUT_BUILD"
}

build_python_one() {
    echo "  > Building ${py_modules[i]} <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/${py_tags[i]}' < " >"$OUT_MSG"

    docker build --target production -t "$TAG_PREFIX/${py_tags[i]}" --build-arg "TARGET_UNIT=${py_packages[i]}" \
      --build-arg "TARGET_MODULE=${py_modules[i]}" "$@" . 2>"$OUT_BUILD"
}

build_python() {
  # Check if we have an argument specifying a single component (tag)
  if [ -n "$1" ]; then
    if [ "$1" == "$STANDALONE_INPUT_TAG" ]; then
      cd standalone_input || return 1
      build_python_standalone_input "$@"
      exit 0
    fi

    if [ "$1" == "$CONFIG_MANAGER_TAG" ]; then
      cd config_manager || return 1
      build_python_config_manager "$@"
      exit 0
    fi

    for i in "${!py_tags[@]}"; do
      if [ "${py_tags[i]}" == "$1" ] ; then
        shift 1
        cd python_pipeline || return 1
        build_python_one "$@"
        exit 0
      fi
    done
  fi

  echo ">>> Building images for Python-based pipeline components <<<" >"$OUT_MSG"
  cd python_pipeline || return 1
  for i in "${!py_packages[@]}"; do
    build_python_one "$@"
  done

  cd ../standalone_input || return 1
  build_python_standalone_input "$@"

  cd ../config_manager || return 1
  build_python_config_manager "$@"

  cd ..
}

if [ "$1" = "help" ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
  echo "Usage: $0 [-q|-qb] [java|python [<component tag>|all]] [additional docker build options]"
  echo "  -q: quiet mode, suppresses all output"
  echo "  -qb: quiet build mode, suppresses Docker build output"
  echo ""
  echo "Builds container images for the Java and Python pipeline components."
  echo "If no component type is specified, builds all components."
  echo "The tag prefix is '$TAG_PREFIX/'."
  echo "You can build a single image by specifying the tag (without the tag prefix)."
  echo ""
  echo "Available Python component tags:"
  for i in "${!py_packages[@]}"; do
    echo "- ${py_tags[i]}"
  done
  echo "- $STANDALONE_INPUT_TAG"
  echo "- $CONFIG_MANAGER_TAG"
  echo "Available Java component tags:"
  echo "- $STREAMS_TAG"
  echo "- $PARCON_TAG"
  echo "- $CONNECT_TAG"
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

if [ "$1" = "java" ]; then
  shift 1
  build_java "$@"
elif [ "$1" = "python" ]; then
  shift 1
  build_python "$@"
else
  if [ "$1" = "all" ]; then
    shift 1
  fi

  build_java "$@"
  build_python "$@"
fi
