#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=duckdb-dest
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
CI=${CI:-"false"}
DATABASE_TYPE="duckdb"
DB_PATH=$SCRIPT_DIR/elements.db

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
function cleanup {
  # Local file cleanup
  cleanup_dir "$WORK_DIR"
  cleanup_dir "$OUTPUT_DIR"
  rm -rf "$DB_PATH"
  if [ "$CI" == "true" ]; then
    cleanup_dir "$DOWNLOAD_DIR"

  fi
}

trap cleanup EXIT

# Create sql instance and create `elements` class
echo "Creating DuckDB DB instance"
# shellcheck source=/dev/null
"$SCRIPT_DIR"/env_setup/duckdb/create-duckdb-instance.sh "$DATABASE_TYPE" "$DB_PATH"
wait

PYTHONPATH=. ./unstructured_ingest/main.py \
  local \
  --num-processes "$max_processes" \
  --output-dir "$OUTPUT_DIR" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/pdf/fake-memo.pdf \
  --work-dir "$WORK_DIR" \
  duckdb \
  --db-type "$DATABASE_TYPE" \
  --database "$DB_PATH"

"$SCRIPT_DIR"/env_setup/duckdb/test-outputs.py \
  --database-type $DATABASE_TYPE \
  --database-path $DB_PATH
