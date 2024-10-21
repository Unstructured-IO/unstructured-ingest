#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=astradb
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
DOWNLOAD_DIR=$SCRIPT_DIR/download/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
CI=${CI:-"false"}

if [ -z "$ASTRA_DB_APPLICATION_TOKEN" ]; then
  echo "Skipping Astra DB source test because ASTRA_DB_APPLICATION_TOKEN env var is not set."
  exit 0
fi

if [ -z "$ASTRA_DB_API_ENDPOINT" ]; then
  echo "Skipping Astra DB source test because ASTRA_DB_API_ENDPOINT env var is not set."
  exit 0
fi

COLLECTION_NAME="ingest_test_src"

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}
PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  astradb \
  --api-key "$UNS_PAID_API_KEY" \
  --partition-by-api \
  --partition-endpoint "https://api.unstructuredapp.io" \
  --metadata-exclude coordinates,filename,file_directory,metadata.last_modified,metadata.data_source.date_processed,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth \
  --token "$ASTRA_DB_APPLICATION_TOKEN" \
  --api-endpoint "$ASTRA_DB_API_ENDPOINT" \
  --collection-name "$COLLECTION_NAME" \
  --num-processes "$max_processes" \
  --download-dir "$DOWNLOAD_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --work-dir "$WORK_DIR" \
  --strategy fast \
  --preserve-downloads \
  --reprocess \
  --batch-size 2 \
  --verbose

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name $OUTPUT_FOLDER_NAME
