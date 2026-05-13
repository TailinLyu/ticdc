#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-100}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-4}"
TIMEOUT="${TIMEOUT:-180}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s29_prefix_${RUN_ID}}"
CF="${CF:-s29-prefix-${RUN_ID}}"
OWNER_PREFIX="${OWNER_PREFIX:-allowed/ticdc-locks-${RUN_ID}}"

cleanup() {
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy
rm -rf "${WAREHOUSE:?}/${OWNER_PREFIX}"
default_markers_before="$(find "${WAREHOUSE}/.ticdc/iceberg-target-owners" -name '*.lock' -type f 2>/dev/null | wc -l | tr -d ' ')"

go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers 1 \
  --batch "${BATCH}" \
  --split-regions "${SPLIT_REGIONS}" \
  --reset \
  --prepare-only >/tmp/"${DB}"-prepare.json

create_feed "${CF}" "${DB}.orders" "_cdc" ticdc-1 "&owner-marker-prefix=${OWNER_PREFIX}"
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
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT:-120}")"
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

custom_markers="$(find "${WAREHOUSE}/${OWNER_PREFIX}" -name '*.lock' -type f 2>/dev/null | wc -l | tr -d ' ')"
default_markers="$(find "${WAREHOUSE}/.ticdc/iceberg-target-owners" -name '*.lock' -type f 2>/dev/null | wc -l | tr -d ' ')"
if [[ "${custom_markers}" == "0" ]]; then
  printf 'expected custom owner markers under %s\n' "${OWNER_PREFIX}" >&2
  exit 1
fi
if [[ "${default_markers}" != "${default_markers_before}" ]]; then
  printf 'expected default owner marker count to stay %s, found %s\n' "${default_markers_before}" "${default_markers}" >&2
  exit 1
fi

remove_feed "${CF}"
trap - EXIT
sleep 5

custom_after="$(find "${WAREHOUSE}/${OWNER_PREFIX}" -name '*.lock' -type f 2>/dev/null | wc -l | tr -d ' ')"
staged_after_remove="$(wait_stage_file_count 0 60)"
if [[ "${custom_after}" != "0" ]]; then
  printf 'expected custom owner markers to be cleaned, found %s\n' "${custom_after}" >&2
  exit 1
fi

printf 'S29_OWNER_PREFIX_PASS cf=%s summary=%s owner_prefix=%s custom_markers=%s custom_after=%s staged_after=%s staged_after_remove=%s\n' \
  "${CF}" \
  "${summary}" \
  "${OWNER_PREFIX}" \
  "${custom_markers}" \
  "${custom_after}" \
  "${staged_after}" \
  "${staged_after_remove}"
