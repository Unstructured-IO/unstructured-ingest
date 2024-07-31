#!/usr/bin/env bash

set -euo pipefail
if [ "${ARCH}" = "amd64" ]; then
  export DOCKER_ARCH=x86_64
elif [ "${ARCH}" = "arm64" ]; then
  export DOCKER_ARCH=aarch64
fi
TARGETOS=linux
DOCKER_VERSION=26.1.3

curl -fLo docker.tgz https://download.docker.com/${TARGETOS}/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz
tar zxvf docker.tgz
rm -rf docker.tgz
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fLo /usr/local/lib/docker/cli-plugins/docker-buildx "https://github.com/docker/buildx/releases/download/v${BUILDX_VERSION}/buildx-v${BUILDX_VERSION}.linux-${TARGETARCH}"
chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
curl -SL https://github.com/docker/compose/releases/download/v2.26.1/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
