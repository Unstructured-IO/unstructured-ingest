#!/usr/bin/env bash

set -e

if [ -z "$1" ]; then
  echo "usage: $0 [location of db file]"
  exit 1
fi

chroma_port=${CHROMA_PORT:-9999}
chroma_host=${CHROMA_HOST:-'localhost'}
# $1 is the path for chroma to write the contents to. The symbol "&" runs process in background
echo "Starting chroma at http://${chroma_host}:${chroma_port} using $1 for db file"
chroma run --port "$chroma_port" --host "$chroma_host" --path "$1" &
