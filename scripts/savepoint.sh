#!/usr/bin/env bash
#
# Trigger a Flink savepoint via REST API and (optionally) cancel the job.
#
# Usage:
#   ./scripts/savepoint.sh <job-id> [--cancel]
#
# Env vars:
#   FLINK_REST   Flink JobManager REST endpoint   (default: http://localhost:8081)
#   TARGET_DIR   Savepoint target directory       (default: file:///tmp/practice-flink-savepoints)
#
# Restore from a savepoint when starting a job:
#   flink run -s <savepoint-path> <jar> ...
#
set -euo pipefail

JOB_ID="${1:-}"
CANCEL_FLAG="${2:-}"

if [[ -z "${JOB_ID}" ]]; then
  echo "Usage: $0 <job-id> [--cancel]" >&2
  exit 1
fi

FLINK_REST="${FLINK_REST:-http://localhost:8081}"
TARGET_DIR="${TARGET_DIR:-file:///tmp/practice-flink-savepoints}"

cancel_job=false
if [[ "${CANCEL_FLAG}" == "--cancel" ]]; then
  cancel_job=true
fi

echo "Triggering savepoint for job ${JOB_ID} (cancel=${cancel_job}) -> ${TARGET_DIR}"

TRIGGER_RESPONSE=$(curl -fsS -X POST \
  -H "Content-Type: application/json" \
  -d "{\"target-directory\":\"${TARGET_DIR}\",\"cancel-job\":${cancel_job}}" \
  "${FLINK_REST}/jobs/${JOB_ID}/savepoints")

TRIGGER_ID=$(printf '%s' "${TRIGGER_RESPONSE}" | sed -E 's/.*"request-id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')

if [[ -z "${TRIGGER_ID}" || "${TRIGGER_ID}" == "${TRIGGER_RESPONSE}" ]]; then
  echo "Failed to parse trigger id from response: ${TRIGGER_RESPONSE}" >&2
  exit 1
fi

echo "Trigger id: ${TRIGGER_ID}"

STATUS_URL="${FLINK_REST}/jobs/${JOB_ID}/savepoints/${TRIGGER_ID}"
while :; do
  RESPONSE=$(curl -fsS "${STATUS_URL}")
  STATUS=$(printf '%s' "${RESPONSE}" | sed -E 's/.*"status"[[:space:]]*:[[:space:]]*\{[[:space:]]*"id"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')
  if [[ "${STATUS}" == "COMPLETED" ]]; then
    echo "${RESPONSE}"
    exit 0
  fi
  if [[ "${STATUS}" == "FAILED" ]]; then
    echo "Savepoint failed: ${RESPONSE}" >&2
    exit 1
  fi
  sleep 2
done
