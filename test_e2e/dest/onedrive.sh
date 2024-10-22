#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1

# TODO: Define output and work directories
OUTPUT_FOLDER_NAME=onedrive_destination
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME

# Set the number of processes and CI flag
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
CI=${CI:-"false"}

# Source cleanup functions
# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh

# Define the cleanup function
function cleanup() {
  cleanup_dir "$OUTPUT_DIR"
  cleanup_dir "$WORK_DIR"

  # Cleanup: Delete the uploaded file from OneDrive
  python "$SCRIPT_DIR"/python/test_onedrive_output.py delete \
    --client-id "$MS_CLIENT_ID" \
    --client-secret "$MS_CLIENT_CRED" \
    --tenant-id "$MS_TENANT_ID" \
    --user-pname "$MS_USER_PNAME" \
    --destination-path "$DESTINATION_PATH"
}

# Set the trap to call cleanup on EXIT
trap cleanup EXIT

# Check if required environment variables are set
if [ -z "$MS_CLIENT_ID" ] || [ -z "$MS_CLIENT_CRED" ] || [ -z "$MS_USER_PNAME" ] || [ -z "$MS_TENANT_ID" ]; then
  echo "Skipping OneDrive destination test because the MS_CLIENT_ID, MS_CLIENT_CRED, MS_USER_PNAME, or MS_TENANT_ID env var is not set."
  exit 0
fi

# Define the ingest script path
RUN_SCRIPT=${RUN_SCRIPT:-./ingest/main.py}

# Generate a unique destination path
UUID=$(python -c 'import uuid; print(uuid.uuid4())')
DESTINATION_FOLDER="/unstructured_ingest_test/$UUID"
DESTINATION_PATH="$DESTINATION_FOLDER/fake-memo.pdf"

PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  local \
  --num-processes "$max_processes" \
  --output-dir "$OUTPUT_DIR" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/pdf/fake-memo.pdf \
  --work-dir "$WORK_DIR" \
  onedrive \
  --client-cred "$MS_CLIENT_CRED" \
  --client-id "$MS_CLIENT_ID" \
  --tenant "$MS_TENANT_ID" \
  --user-pname "$MS_USER_PNAME" \
  --remote-url "$DESTINATION_FOLDER"

# Check that the file was uploaded to OneDrive
python "$SCRIPT_DIR"/python/test-onedrive-output.py check \
  --client-id "$MS_CLIENT_ID" \
  --client-secret "$MS_CLIENT_CRED" \
  --tenant-id "$MS_TENANT_ID" \
  --user-pname "$MS_USER_PNAME" \
  --destination-path "$DESTINATION_PATH"