#!/usr/bin/env bash

set -e

SRC_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$SRC_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=kafka
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
DOWNLOAD_DIR=$OUTPUT_ROOT/download/$OUTPUT_FOLDER_NAME

CI=${CI:-"false"}

RANDOM_SUFFIX=$((RANDOM % 100000 + 1))
LC_ALL=C
#DO not change this name, it needs to match expected output file name in ../expected-structured-output/kafka/
KAFKA_TOPIC="fake-topic"
KAFKA_TOPIC=${KAFKA_TOPIC:-"ingest-test-$RANDOM_SUFFIX"}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
# shellcheck disable=SC2317
function cleanup() {
  cleanup_dir "$OUTPUT_DIR"
  cleanup_dir "$WORK_DIR"
  if [ "$CI" == "true" ]; then
    echo "here"
    cleanup_dir "$DOWNLOAD_DIR"
  fi

  echo "Stopping local Kafka instance"
  docker compose -f "$SCRIPT_DIR"/env_setup/kafka/docker-compose.yml down --remove-orphans -v

}
trap cleanup EXIT

echo "Creating local Kafka instance"
# shellcheck source=/dev/null
"$SCRIPT_DIR"/env_setup/kafka/create-kafka-instance.sh
wait

echo "Sending test document (pdf)"
#Check the number of messages in destination topic
#Note we are calling it twice since this will hack our way into the topic being created (default kafka setting)
python "$SCRIPT_DIR"/python/test-produce-kafka-message.py up \
  --input-file "example-docs/pdf/fake-memo.pdf" \
  --bootstrap-server localhost \
  --topic "$KAFKA_TOPIC" \
  --confluent false \
  --port 29092
python "$SCRIPT_DIR"/python/test-produce-kafka-message.py up \
  --input-file "example-docs/pdf/fake-memo.pdf" \
  --bootstrap-server localhost \
  --topic "$KAFKA_TOPIC" \
  --confluent false \
  --port 29092

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}
PYTHONPATH=${PYTHONPATH:-.} "$RUN_SCRIPT" \
  kafka \
  --bootstrap-server localhost \
  --download-dir "$DOWNLOAD_DIR" \
  --topic "$KAFKA_TOPIC" \
  --num-messages-to-consume 1 \
  --port 29092 \
  --metadata-exclude coordinates,filename,file_directory,metadata.data_source.date_processed,metadata.last_modified,metadata.detection_class_prob,metadata.parent_id,metadata.category_depth \
  --reprocess \
  --output-dir "$OUTPUT_DIR" \
  --verbose \
  --work-dir "$WORK_DIR" \
  --confluent false

"$SCRIPT_DIR"/check-diff-expected-output.py --output-folder-name $OUTPUT_FOLDER_NAME
