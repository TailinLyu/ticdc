#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-10000}"
WORKERS="${WORKERS:-8}"
BATCH="${BATCH:-50}"
BATCH_DELAY="${BATCH_DELAY:-0s}"
SPLIT_REGIONS="${SPLIT_REGIONS:-8}"
TIMEOUT="${TIMEOUT:-600}"
STAGE_TIMEOUT="${STAGE_TIMEOUT:-240}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s12_drain_${RUN_ID}}"
CF="${CF:-s12-drain-${RUN_ID}}"

cleanup() {
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy
wait_stage_file_count 0 30 >/dev/null

go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers 1 \
  --batch "${BATCH}" \
  --split-regions "${SPLIT_REGIONS}" \
  --reset \
  --prepare-only >/tmp/"${DB}"-prepare.json

create_feed "${CF}" "${DB}.orders" "_cdc" ticdc-1
wait_feed_normal "${CF}" 120

go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers "${WORKERS}" \
  --batch "${BATCH}" \
  --batch-delay "${BATCH_DELAY}" \
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_rows=$((ROWS + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT}")"
wait_feed_normal "${CF}" 120

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf 'S12_DRAIN_PASS cf=%s summary=%s staged_after=%s staged_after_remove=%s\n' \
  "${CF}" "${summary}" "${staged_after}" "${staged_after_remove}"
