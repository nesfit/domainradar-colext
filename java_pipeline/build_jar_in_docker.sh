#!/bin/bash

TAG=domrad/java-build

docker build -t "$TAG" --target build --progress=plain .
docker run --rm -v "$PWD":/outdir "$TAG" /bin/bash -c "cp target.jar /outdir/\$(cat /src/artifact_name.txt)"
docker image rm "$TAG"
