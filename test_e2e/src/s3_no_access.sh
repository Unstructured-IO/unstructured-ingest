#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=s3
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
DOWNLOAD_DIR=$SCRIPT_DIR/download/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
# shellcheck disable=SC2317
function cleanup() {
  cleanup_dir "$OUTPUT_DIR"
  cleanup_dir "$WORK_DIR"
}
trap cleanup EXIT

set +e

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}

# Capture the stderr in a variable to check against
{ err=$(PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  s3 \
  --num-processes "$max_processes" \
  --download-dir "$DOWNLOAD_DIR" \
  --reprocess \
  --output-dir "$OUTPUT_DIR" \
  --verbose \
  --remote-url s3://utic-ingest-test-fixtures/destination/ \
  --anonymous \
  --work-dir "$WORK_DIR" 2>&1 >&3 3>&-); } 3>&1

if [[ "$err" == *"Error: Precheck failed"* ]]; then
  echo "passed"
else
  echo "error didn't occur with expected text: $err"
  exit 1
fi
