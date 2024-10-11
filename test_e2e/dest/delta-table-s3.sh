#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=delta-table-dest
OUTPUT_DIR=$SCRIPT_DIR/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$SCRIPT_DIR/workdir/$OUTPUT_FOLDER_NAME
DESTINATION_TABLE=s3://utic-platform-test-destination/test-delta-tables/
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
CI=${CI:-"false"}

if [ -z "$S3_INGEST_TEST_ACCESS_KEY" ] || [ -z "$S3_INGEST_TEST_SECRET_KEY" ]; then
  echo "Skipping test because S3_INGEST_TEST_ACCESS_KEY or S3_INGEST_TEST_SECRET_KEY env var is not set."
  exit 8
fi

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh

function cleanup() {
  echo "--- Running cleanup of s3 location: $DESTINATION_S3 ---"
  if AWS_ACCESS_KEY_ID="$S3_INGEST_TEST_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_INGEST_TEST_SECRET_KEY" aws s3 ls "$DESTINATION_TABLE" --region us-east-2; then
    AWS_ACCESS_KEY_ID="$S3_INGEST_TEST_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$S3_INGEST_TEST_SECRET_KEY" aws s3 rm "$DESTINATION_TABLE" --recursive --region us-east-2
  fi
  echo "--- Cleanup done ---"
}

trap cleanup EXIT

PYTHONPATH=. ./unstructured_ingest/main.py \
  local \
  --num-processes "$max_processes" \
  --output-dir "$OUTPUT_DIR" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/pdf/fake-memo.pdf \
  --work-dir "$WORK_DIR" \
  delta-table \
  --table-uri "$DESTINATION_TABLE" \
  --aws-access-key-id "$S3_INGEST_TEST_ACCESS_KEY" \
  --aws-secret-access-key "$S3_INGEST_TEST_SECRET_KEY"

python "$SCRIPT_DIR"/python/test-ingest-delta-table-output.py \
  --table-uri "$DESTINATION_TABLE" \
  --aws-access-key-id "$S3_INGEST_TEST_ACCESS_KEY" \
  --aws-secret-access-key "$S3_INGEST_TEST_SECRET_KEY"
