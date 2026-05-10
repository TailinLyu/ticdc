#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-50}"
WORKERS="${WORKERS:-2}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-2}"
TIMEOUT="${TIMEOUT:-240}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s17_schema_${RUN_ID}}"
CF="${CF:-s17-schema-${RUN_ID}}"
RULE="${RULE:-${DB}.orders}"
PASS_LABEL="${PASS_LABEL:-S17_SCHEMA_EVOLUTION_PASS}"

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

create_feed "${CF}" "${RULE}" "_cdc" ticdc-1
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
expected_inserts="${ROWS}"
expected_rows=$((expected_inserts + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${expected_inserts}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

go run ./local-e2e/tidbexec "ALTER TABLE ${DB}.orders ADD COLUMN extra VARCHAR(32)"
go run ./local-e2e/tidbexec "INSERT INTO ${DB}.orders (id, customer, amount, paid, note, extra) VALUES (900000001, 'schema-add-1', 1.1, 1, 'after add 1', 'extra-1'), (900000002, 'schema-add-2', 2.2, 0, 'after add 2', 'extra-2')"
expected_inserts=$((expected_inserts + 2))
expected_rows=$((expected_rows + 2))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${expected_inserts}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

schema_after_add="$(go run ./local-e2e/icebergread --schema --warehouse file:///tmp/iceberg-warehouse --catalog http://127.0.0.1:8181 --namespace "${DB}" --table orders_cdc)"
grep -q '^data\.extra[[:space:]]' <<<"${schema_after_add}"
grep -q '^old\.extra[[:space:]]' <<<"${schema_after_add}"

go run ./local-e2e/tidbexec "ALTER TABLE ${DB}.orders RENAME COLUMN extra TO extra_renamed"
go run ./local-e2e/tidbexec "INSERT INTO ${DB}.orders (id, customer, amount, paid, note, extra_renamed) VALUES (900000003, 'schema-rename-1', 3.3, 1, 'after rename 1', 'extra-renamed-1'), (900000004, 'schema-rename-2', 4.4, 0, 'after rename 2', 'extra-renamed-2')"
expected_inserts=$((expected_inserts + 2))
expected_rows=$((expected_rows + 2))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${expected_inserts}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

schema_after_rename="$(go run ./local-e2e/icebergread --schema --warehouse file:///tmp/iceberg-warehouse --catalog http://127.0.0.1:8181 --namespace "${DB}" --table orders_cdc)"
grep -q '^data\.extra_renamed[[:space:]]' <<<"${schema_after_rename}"
grep -q '^old\.extra_renamed[[:space:]]' <<<"${schema_after_rename}"

go run ./local-e2e/tidbexec "ALTER TABLE ${DB}.orders DROP COLUMN note"
go run ./local-e2e/tidbexec "INSERT INTO ${DB}.orders (id, customer, amount, paid, extra_renamed) VALUES (900000005, 'schema-drop-1', 5.5, 1, 'extra-drop-1'), (900000006, 'schema-drop-2', 6.6, 0, 'extra-drop-2')"
expected_inserts=$((expected_inserts + 2))
expected_rows=$((expected_rows + 2))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${expected_inserts}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

schema_after_drop="$(go run ./local-e2e/icebergread --schema --warehouse file:///tmp/iceberg-warehouse --catalog http://127.0.0.1:8181 --namespace "${DB}" --table orders_cdc)"
grep -q '^data\.note[[:space:]]' <<<"${schema_after_drop}"
grep -q '^old\.note[[:space:]]' <<<"${schema_after_drop}"

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf '%s cf=%s rule=%s summary=%s staged_after_remove=%s schema_add=ok schema_rename=ok schema_drop_absorbed=ok\n' \
  "${PASS_LABEL}" "${CF}" "${RULE}" "${summary}" "${staged_after_remove}"
