#!/usr/bin/env bash

set -euo pipefail

DOCKER_ARCH=${ARCH}
if [ "${ARCH}" = "x86_64" ]; then
  TARGETARCH="amd64"
elif [ "${ARCH}" = "arm64" ] || [ "${ARCH}" = "aarch64" ]; then
  TARGETARCH="arm64"
fi
TARGETOS=linux
DOCKER_VERSION=26.1.3
BUILDX_VERSION=0.16.0
DOCKER_COMPOSE_VERSION=2.28.1

curl -fLo docker.tgz https://download.docker.com/${TARGETOS}/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz
tar zxvf docker.tgz
rm -rf docker.tgz
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fLo /usr/local/lib/docker/cli-plugins/docker-buildx "https://github.com/docker/buildx/releases/download/v${BUILDX_VERSION}/buildx-v${BUILDX_VERSION}.linux-${TARGETARCH}"
chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx
curl -SL https://github.com/docker/compose/releases/download/v${DOCKER_COMPOSE_VERSION}/docker-compose-${TARGETOS}-${DOCKER_ARCH} -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
