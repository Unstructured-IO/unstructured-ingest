#!/usr/bin/env bash

DEST_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$DEST_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=s3-dest
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
DESTINATION_S3="s3://utic-ingest-test-fixtures/destination/no_access/$(uuidgen)/"
CI=${CI:-"false"}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
function cleanup() {
  cleanup_dir "$WORK_DIR"
}
trap cleanup EXIT

set +e

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}

# Capture the stderr in a variable to check against
{ err=$(PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  local \
  --num-processes "$max_processes" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/pdf/fake-memo.pdf \
  --work-dir "$WORK_DIR" \
  s3 \
  --remote-url "$DESTINATION_S3" 2>&1 >&3 3>&-); } 3>&1

if [[ "$err" == *"Error: Precheck failed"* ]]; then
  echo "passed"
else
  echo "error didn't occur with expected text: $err"
  exit 1
fi
