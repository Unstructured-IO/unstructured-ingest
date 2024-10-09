#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=local-single-file-with-pdf-infer-table-structure
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
# shellcheck disable=SC2317
function cleanup() {
	cleanup_dir "$OUTPUT_DIR"
	cleanup_dir "$WORK_DIR"
}
trap cleanup EXIT

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}
PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
local \
--api-key "$UNS_PAID_API_KEY" \
--partition-by-api \
--partition-endpoint "https://api.unstructuredapp.io" \
--num-processes "$max_processes" \
--metadata-exclude coordinates,filename,file_directory,metadata.data_source.date_created,metadata.data_source.date_modified,metadata.data_source.date_processed,metadata.last_modified,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth \
--output-dir "$OUTPUT_DIR" \
--skip-infer-table-types "xls,xlsx" \
--strategy hi_res \
--verbose \
--reprocess \
--input-path "$SCRIPT_DIR"/example-docs/ \
--work-dir "$WORK_DIR"

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name "$OUTPUT_FOLDER_NAME"
