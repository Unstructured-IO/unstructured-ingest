#!/usr/bin/env bash

set -e

DEST_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$DEST_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=kdbai-dest
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
CI=${CI:-"false"}
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}

if [ -z "$KDBAI_CIPHER_KEY" ] || [ -z "$KDBAI_USERNAME" ] || [ -z "$KDBAI_BEARER_TOKEN" ]; then
  echo "Skipping KDBAI ingest test because KDBAI_CIPHER_KEY, KDBAI_USERNAME or KDBAI_BEARER_TOKEN env vars is not set."
  exit 8
fi

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
function cleanup {

  echo "Stopping KDBAI Docker container"
  docker compose -f "$SCRIPT_DIR"/env_setup/kdbai/docker-compose.yml down --remove-orphans -v

  # Local file cleanup
  cleanup_dir "$WORK_DIR"
  cleanup_dir "$OUTPUT_DIR"

}

trap cleanup EXIT

# Fetch docker image from kdbai private registry
docker login portal.dl.kx.com -u "$KDBAI_USERNAME" -p "$KDBAI_BEARER_TOKEN"
docker pull portal.dl.kx.com/kdbai-db:latest
docker logout

"$SCRIPT_DIR"/env_setup/kdbai/provision.sh

PYTHONPATH=. ./unstructured/ingest/main.py \
  local \
  --num-processes "$max_processes" \
  --output-dir "$OUTPUT_DIR" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/fake-memo.pdf \
  --work-dir "$WORK_DIR" \
  --embedding-provider "langchain-huggingface" \
  kdbai \
  --table-name "unstructured_test" \
  --batch-size 100

python "$SCRIPT_DIR"/env_setup/kdbai/test_output.py
