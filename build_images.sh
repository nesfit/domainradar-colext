#!/bin/bash

# Usage: ./build_images.sh [-q|-qb] [java|python [<component tag>|all]] [additional docker build options]"
#   -q: quiet mode, suppresses all output"
#   -qb: quiet build mode, suppresses Docker build output"
#
# Builds container images for the Java and Python pipeline components.
# If no component type is specified, builds all components.
# The tag prefix is '$TAG_PREFIX/'.
# You can build a single image by specifying the tag (without the tag prefix).

TAG_PREFIX="thor"

COL_JAVA_TAG="collector-java"
COL_PY_TAG="collector-py"
CONNECT_TAG="kafka-connect"

OUT_BUILD=/dev/stdout
OUT_MSG=/dev/stdout

build_java() {
  echo ">>> Building images for Java-based pipeline components <<<" >"$OUT_MSG"
  cd collectors-java || return 1

  local build_parcon=1
  local build_connect=1

  if [ -n "$1" ]; then
    if [ "$1" == "$COL_JAVA_TAG" ]; then
      build_connect=0
      shift 1
    elif [ "$1" == "$CONNECT_TAG" ]; then
      build_parcon=0
      shift 1
    fi
  fi

  if [ "$build_parcon" == 1 ]; then
    echo "  > Building Java (Parallel Consumer) collectors <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/$COL_JAVA_TAG' < " >"$OUT_MSG"

    docker build -f Dockerfile -t "$TAG_PREFIX/$COL_JAVA_TAG" "$@" . 2>"$OUT_BUILD"
  fi

  if [ "$build_connect" == 1 ]; then
    echo "  > Building Kafka Connect base image <  " >"$OUT_MSG"
    echo "    > Tag: '$TAG_PREFIX/$CONNECT_TAG' < " >"$OUT_MSG"

    docker build -f connect.Dockerfile --target runtime-connect -t "$TAG_PREFIX/$CONNECT_TAG" "$@" . 2>"$OUT_BUILD"
  fi

  cd ..
}


build_python() {
  echo ">>> Building images for Python-based pipeline components <<<" >"$OUT_MSG"
  cd collectors-python || return 1

  echo "  > Building Python collectors <  " >"$OUT_MSG"
  echo "    > Tag: '$TAG_PREFIX/$COL_PY_TAG' < " >"$OUT_MSG"

  docker build -f Dockerfile -t "$TAG_PREFIX/$COL_PY_TAG" "$@" . 2>"$OUT_BUILD"
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
  echo "- $COL_PY_TAG"
  echo "Available Java component tags:"
  echo "- $COL_JAVA_TAG"
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
