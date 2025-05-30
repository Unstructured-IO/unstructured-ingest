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

"$SCRIPT_DIR"/check-num-files-expected-output.sh 3 $OUTPUT_FOLDER_NAME 20k

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}
PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  s3 \
  --api-key "$UNS_PAID_API_KEY" \
  --partition-by-api \
  --partition-endpoint "https://api.unstructuredapp.io" \
  --num-processes "$max_processes" \
  --download-dir "$DOWNLOAD_DIR" \
  --metadata-exclude coordinates,filename,file_directory,metadata.data_source.date_processed,metadata.last_modified,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth \
  --strategy hi_res \
  --preserve-downloads \
  --reprocess \
  --verbose \
  --remote-url s3://utic-dev-tech-fixtures/small-pdf-set/ \
  --anonymous \
  --work-dir "$WORK_DIR" \
  local \
  --output-dir "$OUTPUT_DIR"

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name $OUTPUT_FOLDER_NAME
