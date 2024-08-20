#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "$(realpath "$0")")

gpg --quiet --batch --yes --decrypt --passphrase="$KDBAI_CIPHER_KEY" --output "$SCRIPT_DIR"/k4.lic "$SCRIPT_DIR"/k4.lic.gpg

# Create the Opensearch cluster
docker compose version
docker compose -f "$SCRIPT_DIR"/docker-compose.yaml up --wait
docker compose -f "$SCRIPT_DIR"/docker-compose.yaml ps

echo "Cluster is live."
"$SCRIPT_DIR"/provision.py
