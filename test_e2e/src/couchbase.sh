#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=couchbase
OUTPUT_DIR=$SCRIPT_DIR/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$SCRIPT_DIR/workdir/$OUTPUT_FOLDER_NAME
DOWNLOAD_DIR=$SCRIPT_DIR/download/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
CI=${CI:-"false"}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/env_setup/couchbase/common/constants.env

# Check if all necessary environment variables are set
if [ -z "$CB_USERNAME" ] || [ -z "$CB_PASSWORD" ] || [ -z "$CB_CONN_STR" ] || [ -z "$CB_BUCKET" ] || [ -z "$CB_SCOPE" ] || [ -z "$CB_COLLECTION" ] || [ -z "$CB_COLLECTION_ID" ]; then
	echo "Error: One or more environment variables are not set. Please set CB_CONN_STR, CB_USERNAME, CB_PASSWORD, CB_BUCKET, CB_SCOPE, CB_COLLECTION and CB_COLLECTION_ID."
	exit 1
fi

function cleanup() {
	# Remove docker container
	echo "Stopping Couchbase Docker container"
	docker compose -f "$SCRIPT_DIR"/env_setup/couchbase/common/docker-compose.yaml down --remove-orphans

	cleanup_dir "$DESTINATION_PATH"
	cleanup_dir "$OUTPUT_DIR"
	cleanup_dir "$WORK_DIR"
	if [ "$CI" == "true" ]; then
		cleanup_dir "$DOWNLOAD_DIR"
	fi
}

trap cleanup EXIT

echo "Starting Couchbase Docker container and setup"

bash "$SCRIPT_DIR"/env_setup/couchbase/common/setup_couchbase_cluster.sh
wait

python "$SCRIPT_DIR"/env_setup/couchbase/source_connector/ingest_source_setup_cluster.py \
	--username "$CB_USERNAME" \
	--password "$CB_PASSWORD" \
	--connection_string "$CB_CONN_STR" \
	--bucket_name "$CB_BUCKET" \
	--scope_name "$CB_SCOPE" \
	--collection_name "$CB_COLLECTION" \
	--collection_id "$CB_COLLECTION_ID" \
	--source_file "$SCRIPT_DIR"/env_setup/couchbase/source_connector/airline_sample.jsonlines
wait

PYTHONPATH=. ./unstructured_ingest/main.py \
	couchbase \
	--api-key "$UNS_PAID_API_KEY" \
	--partition-by-api \
	--partition-endpoint "https://api.unstructuredapp.io" \
	--metadata-exclude file_directory,metadata.data_source.date_processed,metadata.last_modified,metadata.date_created,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth \
	--num-processes "$max_processes" \
	--download-dir "$DOWNLOAD_DIR" \
	--connection-string "$CB_CONN_STR" \
	--bucket "$CB_BUCKET" \
	--username "$CB_USERNAME" \
	--password "$CB_PASSWORD" \
	--scope "$CB_SCOPE" \
	--collection "$CB_COLLECTION" \
	--collection-id "$CB_COLLECTION_ID" \
	--work-dir "$WORK_DIR" \
	--preserve-downloads \
	--reprocess \
	--batch-size 2 \
	--verbose \
	local \
	--output-dir "$OUTPUT_DIR"

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name $OUTPUT_FOLDER_NAME
