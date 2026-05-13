#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-500}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-4}"
TIMEOUT="${TIMEOUT:-360}"
STAGE_TIMEOUT="${STAGE_TIMEOUT:-180}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s09_catalog_${RUN_ID}}"
CF="${CF:-s09-catalog-${RUN_ID}}"

cleanup() {
  ${COMPOSE} start iceberg-rest >/dev/null || true
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

${COMPOSE} stop iceberg-rest >/dev/null

go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers "${WORKERS}" \
  --batch "${BATCH}" \
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json

staged_during="$(wait_stage_file_count_at_least 1 "${STAGE_TIMEOUT}")"

${COMPOSE} start iceberg-rest >/dev/null
wait_iceberg_rest 120
wait_feed_normal "${CF}" 180

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_rows=$((ROWS + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT}")"

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf 'S09_CATALOG_RECOVERY_PASS cf=%s summary=%s staged_during=%s staged_after=%s staged_after_remove=%s\n' \
  "${CF}" "${summary}" "${staged_during}" "${staged_after}" "${staged_after_remove}"
