#!/usr/bin/env bash

set -e

DEST_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$DEST_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=onedrive-dest
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}
DESTINATION_ONEDRIVE_FOLDER="test-output-$(uuidgen)"
CI=${CI:-"false"}

# Check for required environment variables
if [ -z "$MS_CLIENT_ID" ] || [ -z "$MS_CLIENT_CRED" ] || [ -z "$MS_TENANT_ID" ] || [ -z "$MS_USER_PNAME" ]; then
  echo "Skipping OneDrive ingest test because one or more of these env vars is not set:"
  echo "MS_CLIENT_ID, MS_CLIENT_CRED, MS_TENANT_ID, MS_USER_PNAME"
  exit 8
fi

# Obtain an access token from Microsoft Graph API using client credentials flow
MS_RESPONSE=$(curl -s -X POST "https://login.microsoftonline.com/$MS_TENANT_ID/oauth2/v2.0/token" \
  -F client_id="$MS_CLIENT_ID" \
  -F scope="https://graph.microsoft.com/.default" \
  -F client_secret="$MS_CLIENT_CRED" \
  -F grant_type=client_credentials)

MS_ACCESS_TOKEN=$(jq -r '.access_token' <<<"$MS_RESPONSE")

if [ -z "$MS_ACCESS_TOKEN" ] || [ "$MS_ACCESS_TOKEN" == "null" ]; then
  echo "Failed to obtain access token from Microsoft Graph API."
  echo "Response: $MS_RESPONSE"
  exit 1
fi

# Source cleanup functions
# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh

function cleanup() {
  cleanup_dir "$OUTPUT_DIR"
  cleanup_dir "$WORK_DIR"

  echo "Deleting test folder $DESTINATION_ONEDRIVE_FOLDER from OneDrive"
  # Delete the folder using Microsoft Graph API
  curl -s -X DELETE \
    "https://graph.microsoft.com/v1.0/users/$MS_USER_PNAME/drive/root:/$DESTINATION_ONEDRIVE_FOLDER" \
    -H "Authorization: Bearer $MS_ACCESS_TOKEN" \
    -H "Content-Type: application/json"
}
trap cleanup EXIT

# Create a new folder for the test in OneDrive
echo "Creating temp directory in OneDrive for testing: $DESTINATION_ONEDRIVE_FOLDER"
response=$(curl -s -X POST -w "\n%{http_code}" \
  "https://graph.microsoft.com/v1.0/users/$MS_USER_PNAME/drive/root/children" \
  -H "Authorization: Bearer $MS_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\": \"$DESTINATION_ONEDRIVE_FOLDER\", \"folder\": {}, \"@microsoft.graph.conflictBehavior\": \"replace\"}")
http_code=$(tail -n1 <<<"$response") # get the last line (HTTP status code)
content=$(sed '$ d' <<<"$response")  # get all but the last line (the response content)

if [ "$http_code" -ge 300 ]; then
  echo "Failed to create temp dir in OneDrive: [$http_code] $content"
  exit 1
else
  echo "$http_code:"
  jq <<<"$content"
fi

RUN_SCRIPT=${RUN_SCRIPT:-./unstructured_ingest/main.py}
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
  --client-id "$MS_CLIENT_ID" \
  --client-cred "$MS_CLIENT_CRED" \
  --tenant "$MS_TENANT_ID" \
  --user-pname "$MS_USER_PNAME" \
  --remote-url "onedrive://$DESTINATION_ONEDRIVE_FOLDER"

# Check the number of files uploaded to OneDrive
expected_num_files=1
num_files_in_onedrive=$(curl -s -X GET \
  "https://graph.microsoft.com/v1.0/users/$MS_USER_PNAME/drive/root:/$DESTINATION_ONEDRIVE_FOLDER:/children" \
  -H "Authorization: Bearer $MS_ACCESS_TOKEN" | jq '.value | length')

if [ "$num_files_in_onedrive" -ne "$expected_num_files" ]; then
  echo "Expected $expected_num_files files to be uploaded to OneDrive, but found $num_files_in_onedrive files."
  exit 1
else
  echo "Successfully uploaded $num_files_in_onedrive file(s) to OneDrive."
fi
