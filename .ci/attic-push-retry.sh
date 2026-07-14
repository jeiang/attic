#!/usr/bin/env bash
# Push store paths to an attic cache, retrying on transient failures.
# The server deduplicates already-uploaded paths, so retries only
# re-upload whatever failed mid-transfer.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <server:cache> <store-path>..." >&2
  exit 64
fi

cache=$1
shift

attempts=3
for attempt in $(seq 1 "$attempts"); do
  if attic push "$cache" "$@"; then
    exit 0
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    echo "attic push failed (attempt $attempt/$attempts); retrying in 15s..." >&2
    sleep 15
  fi
done

echo "attic push failed after $attempts attempts" >&2
exit 1
