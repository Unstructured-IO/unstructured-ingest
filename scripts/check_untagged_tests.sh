#!/usr/bin/env bash

function create_tags {
  echo "--exclude-tags "{blob_storage,sql,nosql,vector_db,graph_db,uncategorized}
}

tags=$(create_tags)
# shellcheck disable=SC2086
missing_tags=$(PYTHONPATH=. pytest --collect-only test/integration/connectors $tags | grep "<Function|<Coroutine" -c)
echo "$missing_tags"
if [ "$missing_tags" -gt 0 ]; then
  echo "Missing tags in integration tests: "
  # shellcheck disable=SC2086
  PYTHONPATH=. pytest --collect-only test/integration/connectors $tags | grep "<Function|<Coroutine"
  exit 1
fi
