#!/bin/bash

# TODO: This is currently not working!

TAG=domrad/java-build

docker build -t "$TAG" --target build_component --progress=plain .
docker run --rm -v "$PWD":/outdir "$TAG" /bin/bash -c "cp target.jar /outdir/\$(cat /src/artifact_name.txt)"
docker image rm "$TAG"
