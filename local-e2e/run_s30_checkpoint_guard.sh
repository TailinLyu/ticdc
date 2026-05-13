#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-300}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-4}"
TIMEOUT="${TIMEOUT:-300}"
STAGE_TIMEOUT="${STAGE_TIMEOUT:-180}"
GUARD_SLEEP="${GUARD_SLEEP:-15}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s30_checkpoint_${RUN_ID}}"
CF="${CF:-s30-checkpoint-${RUN_ID}}"

cleanup() {
  ${COMPOSE} start iceberg-rest >/dev/null || true
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy
${COMPOSE} restart iceberg-rest >/dev/null
wait_iceberg_rest 120
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

warmup_since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
go run ./local-e2e/tidbexec \
  "INSERT INTO ${DB}.orders (id, customer, amount, paid, note) VALUES (1, 'warmup', 1.0, 0, 'warmup')"
warmup_deadline=$((SECONDS + 120))
warmup_append_services=""
while (( SECONDS < warmup_deadline )); do
  warmup_append_services="$(log_services_for "${warmup_since}" "${CF}" "iceberg committer appended staged rows")"
  if [[ -n "${warmup_append_services}" && "$(stage_file_count)" == "0" ]]; then
    break
  fi
  sleep 2
done
if [[ -z "${warmup_append_services}" ]]; then
  printf 'expected warmup append before catalog outage\n' >&2
  exit 1
fi
wait_readback "${DB}" orders_cdc 1 1 0 0 120 >/dev/null

${COMPOSE} stop iceberg-rest >/dev/null

go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers "${WORKERS}" \
  --batch "${BATCH}" \
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json

staged_during="$(wait_stage_file_count_at_least 1 "${STAGE_TIMEOUT}")"
oldest_staged_max_commit_ts="$(oldest_staged_max_commit_ts)"
checkpoint_during="$(changefeed_checkpoint_tso "${CF}")"

sleep "${GUARD_SLEEP}"
checkpoint_after_guard="$(changefeed_checkpoint_tso "${CF}")"
staged_after_guard="$(stage_file_count)"

if [[ "${oldest_staged_max_commit_ts}" == "0" ]]; then
  printf 'expected staged max commit ts while catalog is down\n' >&2
  exit 1
fi
if (( staged_after_guard < 1 )); then
  printf 'expected staged files to remain while catalog is down: checkpoint=%s oldest_staged_max_commit_ts=%s checkpoint_during=%s\n' \
    "${checkpoint_after_guard}" "${oldest_staged_max_commit_ts}" "${checkpoint_during}" >&2
  exit 1
fi

${COMPOSE} start iceberg-rest >/dev/null
wait_iceberg_rest 120
wait_feed_normal "${CF}" 180
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT}")"

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_inserts=$((ROWS + 1))
expected_rows=$((expected_inserts + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${expected_inserts}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf 'S30_CHECKPOINT_GUARD_PASS cf=%s summary=%s staged_during=%s staged_after_guard=%s oldest_staged_max_commit_ts=%s checkpoint_during=%s checkpoint_after_guard=%s staged_after=%s staged_after_remove=%s\n' \
  "${CF}" \
  "${summary}" \
  "${staged_during}" \
  "${staged_after_guard}" \
  "${oldest_staged_max_commit_ts}" \
  "${checkpoint_during}" \
  "${checkpoint_after_guard}" \
  "${staged_after}" \
  "${staged_after_remove}"
