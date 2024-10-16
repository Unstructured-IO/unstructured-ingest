#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(dirname "$(realpath "$0")")
DATABASE_NAME=$1
DATABASE_FILE_PATH=$2

python "$SCRIPT_DIR"/create-duckdb-schema.py "$DATABASE_FILE_PATH"

echo "$DATABASE_NAME instance is live."
