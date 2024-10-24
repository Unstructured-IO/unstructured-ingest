#!/usr/bin/env bash

echo
if [ $# -ne 2 ]; then
  echo "usage: $0 path [source|destination]"
  exit 1
fi

path=$1
type=$2

if ! { [ $type == "source" ] || [ $type == "destination" ]; }; then
  echo "usage: $0 path [source|destination]"
  exit 1
fi

for docker_file in $(find $path -type f -name "docker-compose.yaml" | grep $type); do
  echo "pulling images from $docker_file"
  docker compose -f $docker_file pull
done
