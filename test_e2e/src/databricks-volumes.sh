#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=databricks-volumes
OUTPUT_DIR=$SCRIPT_DIR/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$SCRIPT_DIR/workdir/$OUTPUT_FOLDER_NAME
DOWNLOAD_DIR=$SCRIPT_DIR/download/$OUTPUT_FOLDER_NAME
CI=${CI:-"false"}

RANDOM_SUFFIX=$((RANDOM % 100000 + 1))

DATABRICKS_VOLUME="test-platform"
DATABRICKS_VOLUME_PATH="databricks-volumes-test-output-$RANDOM_SUFFIX"

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
function cleanup() {

  python "$SCRIPT_DIR"/python/test-databricks-volumes.py cleanup \
    --host "$DATABRICKS_HOST" \
    --client-id "$DATABRICKS_CLIENT_ID" \
    --client-secret "$DATABRICKS_CLIENT_SECRET" \
    --volume "$DATABRICKS_VOLUME" \
    --catalog "$DATABRICKS_CATALOG" \
    --volume-path "$DATABRICKS_VOLUME_PATH"

  cleanup_dir "$OUTPUT_DIR"
  cleanup_dir "$WORK_DIR"
  if [ "$CI" == "true" ]; then
    cleanup_dir "$DOWNLOAD_DIR"
  fi
}
trap cleanup EXIT

python "$SCRIPT_DIR"/python/test-databricks-volumes.py upload \
  --host "$DATABRICKS_HOST" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --volume "$DATABRICKS_VOLUME" \
  --catalog "$DATABRICKS_CATALOG" \
  --volume-path "$DATABRICKS_VOLUME_PATH/fake-memo.pdf" \
  --local-filepath example-docs/pdf/fake-memo.pdf

PYTHONPATH=. ./unstructured_ingest/main.py \
  databricks-volumes \
  --host "$DATABRICKS_HOST" \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --volume "$DATABRICKS_VOLUME" \
  --catalog "$DATABRICKS_CATALOG" \
  --volume-path "$DATABRICKS_VOLUME_PATH" \
  --output-dir "$OUTPUT_DIR" \
  --download-dir "$DOWNLOAD_DIR" \
  --strategy fast \
  --verbose \
  --metadata-exclude coordinates,filename,file_directory,metadata.data_source.date_modified,metadata.last_modified,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth,metadata.data_source.url \
  --work-dir "$WORK_DIR"

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name $OUTPUT_FOLDER_NAME
