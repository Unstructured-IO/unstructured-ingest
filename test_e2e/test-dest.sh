#!/usr/bin/env bash

set -u -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
SKIPPED_FILES_LOG=$SCRIPT_DIR/skipped-files.txt
# If the file already exists, reset it
if [ -f "$SKIPPED_FILES_LOG" ]; then
  rm "$SKIPPED_FILES_LOG"
fi
touch "$SKIPPED_FILES_LOG"
cd "$SCRIPT_DIR"/.. || exit 1

# NOTE(crag): sets number of tesseract threads to 1 which may help with more reproducible outputs
export OMP_THREAD_LIMIT=1

all_tests=(
  'azure.sh'
  'box.sh'
  'couchbase.sh'
  'gcs.sh'
  's3.sh'
  'sharepoint-embed-cog-index.sh'
  #  'kdbai.sh'
)

full_python_matrix_tests=(
  'azure.sh'
  'gcs.sh'
  's3.sh'
)

CURRENT_TEST="none"

function print_last_run() {
  if [ "$CURRENT_TEST" != "none" ]; then
    echo "Last ran script: $CURRENT_TEST"
  fi
  echo "######## SKIPPED TESTS: ########"
  cat "$SKIPPED_FILES_LOG"
}

trap print_last_run EXIT

python_version=$(python --version 2>&1)

tests_to_ignore=(
  'sharepoint.sh'
)

for test in "${all_tests[@]}"; do
  CURRENT_TEST="$test"
  # IF: python_version is not 3.10 (wildcarded to match any subminor version) AND the current test is not in full_python_matrix_tests
  # Note: to test we expand the full_python_matrix_tests array to a string and then regex match the current test
  if [[ "$python_version" != "Python 3.10"* ]] && [[ ! "${full_python_matrix_tests[*]}" =~ $test ]]; then
    echo "--------- SKIPPING SCRIPT $test ---------"
    continue
  fi
  echo "--------- RUNNING SCRIPT $test ---------"
  echo "Running ./test_e2e/dest/$test"
  ./test_e2e/dest/"$test"
  rc=$?
  if [[ $rc -eq 8 ]]; then
    echo "$test (skipped due to missing env var)" | tee -a "$SKIPPED_FILES_LOG"
  elif [[ "${tests_to_ignore[*]}" =~ $test ]]; then
    echo "$test (skipped checking error code: $rc)" | tee -a "$SKIPPED_FILES_LOG"
    continue
  elif [[ $rc -ne 0 ]]; then
    exit $rc
  fi
  echo "--------- FINISHED SCRIPT $test ---------"
done
