#!/usr/bin/env bash
# Push store paths to an attic cache, retrying on transient failures.
# The server deduplicates already-uploaded paths, so retries only
# re-upload whatever failed mid-transfer.
#
# A cache push is an optimization, not a build product: exhausting all
# attempts emits a workflow warning and exits 0 so cache outages cannot
# fail CI. Set ATTIC_PUSH_STRICT=1 to restore hard failure.
#
# Each attempt runs under a watchdog: the bootstrap attic client has no
# request timeout, so a black-holed connection hangs `attic push`
# forever instead of erroring. Tune with ATTIC_PUSH_TIMEOUT (seconds).
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <server:cache> <store-path>..." >&2
  exit 64
fi

cache=$1
shift

timeout=${ATTIC_PUSH_TIMEOUT:-1800}
attempts=3

push_with_watchdog() {
  attic push "$cache" "$@" &
  local push_pid=$!
  (
    sleep "$timeout"
    echo "attic push exceeded ${timeout}s; killing hung push" >&2
    kill -TERM "$push_pid" 2>/dev/null
  ) &
  local watchdog_pid=$!
  local status=0
  wait "$push_pid" || status=$?
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$status"
}

for attempt in $(seq 1 "$attempts"); do
  if push_with_watchdog "$@"; then
    exit 0
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    echo "attic push failed (attempt $attempt/$attempts); retrying in 15s..." >&2
    sleep 15
  fi
done

echo "attic push failed after $attempts attempts" >&2
if [ "${ATTIC_PUSH_STRICT:-0}" = "1" ]; then
  exit 1
fi
echo "::warning::attic push failed after $attempts attempts; continuing without cache push"
exit 0
