#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-2000}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-25}"
BATCH_DELAY="${BATCH_DELAY:-20ms}"
SPLIT_REGIONS="${SPLIT_REGIONS:-6}"
TIMEOUT="${TIMEOUT:-300}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s20_restarts_${RUN_ID}}"
CF="${CF:-s20-restarts-${RUN_ID}}"

cleanup() {
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy

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
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json &
workload_pid=$!

restart_sequence=(ticdc-1 ticdc-2 ticdc-3 ticdc-2 ticdc-1 ticdc-3)
for service in "${restart_sequence[@]}"; do
  sleep 2
  ${COMPOSE} restart "${service}" >/dev/null
done

wait "${workload_pid}"
wait_captures 3 120

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_rows=$((ROWS + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT:-120}")"

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(stage_file_count)"
if [[ "${staged_after_remove}" != "0" ]]; then
  printf 'staged_after_remove=%s\n' "${staged_after_remove}" >&2
  exit 1
fi

printf 'S20_ROLLING_PASS cf=%s summary=%s staged_after=%s staged_after_remove=%s\n' \
  "${CF}" "${summary}" "${staged_after}" "${staged_after_remove}"
