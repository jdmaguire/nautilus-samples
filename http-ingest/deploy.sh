#!/bin/bash

TARGET=$1
if [ -z "${TARGET}" ]; then
    echo "Usage: $0 <TARGET_IP>"
    exit 1
fi

IMAGE_REPO=httpingest:latest

set +ex

(cd ..; ./gradlew assemble)

docker build -t $IMAGE_REPO .
docker tag $IMAGE_REPO $TARGET:5000/$IMAGE_REPO
docker push $TARGET:5000/$IMAGE_REPO

dcos marathon app add httpingest-marathon.json