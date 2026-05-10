#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-50}"
WORKERS="${WORKERS:-2}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-2}"
TIMEOUT="${TIMEOUT:-180}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s17_unsupported_${RUN_ID}}"
CF="${CF:-s17-unsupported-${RUN_ID}}"
DDL_SQL="${DDL_SQL:-ALTER TABLE ${DB}.orders ADD COLUMN extra VARCHAR(32)}"

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
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_rows=$((ROWS + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"
staged_before_ddl="$(wait_stage_file_count 0 120)"

go run ./local-e2e/tidbexec "${DDL_SQL}" >/tmp/"${DB}"-ddl.txt

deadline=$((SECONDS + TIMEOUT))
state=""
error_text=""
while (( SECONDS < deadline )); do
  query="$(${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${CF}" 2>/dev/null || true)"
  state="$(jq -r '.state // empty' <<<"${query}")"
  error_text="$(jq -r '[.. | strings] | join(" ")' <<<"${query}")"
  if [[ "${state}" == "warning" && "${error_text}" == *"iceberg sink does not support block event"* ]]; then
    break
  fi
  sleep 2
done

if [[ "${state}" != "warning" || "${error_text}" != *"iceberg sink does not support block event"* ]]; then
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${CF}" || true
  exit 1
fi

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf 'S17_SCHEMA_UNSUPPORTED_PASS cf=%s summary=%s staged_before_ddl=%s state=%s staged_after_remove=%s\n' \
  "${CF}" "${summary}" "${staged_before_ddl}" "${state}" "${staged_after_remove}"
