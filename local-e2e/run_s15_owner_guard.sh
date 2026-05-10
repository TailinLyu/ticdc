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
DB="${DB:-ice_s15_owner_${RUN_ID}}"
LEFT_CF="${LEFT_CF:-s15-owner-left-${RUN_ID}}"
RIGHT_CF="${RIGHT_CF:-s15-owner-right-${RUN_ID}}"
TARGET_TABLE="${TARGET_TABLE:-orders_shared}"

cleanup() {
  remove_feed "${LEFT_CF}" || true
  remove_feed "${RIGHT_CF}" || true
}
trap cleanup EXIT

owner_conflict_metric_count() {
  for port in 8300 8301 8302; do
    curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null |
      awk '/^ticdc_sink_iceberg_target_owner_conflicts_total/ {print $NF}' || true
  done | awk '{sum += $1} END {printf "%.0f", sum}'
}

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

create_feed "${LEFT_CF}" "${DB}.orders" "_shared" ticdc-1
create_feed "${RIGHT_CF}" "${DB}.orders" "_shared" ticdc-2
wait_feed_normal "${LEFT_CF}" 120
wait_feed_normal "${RIGHT_CF}" 120

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
summary="$(wait_readback "${DB}" "${TARGET_TABLE}" "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

deadline=$((SECONDS + TIMEOUT))
left_state=""
right_state=""
while ((SECONDS < deadline)); do
  left_state="$(${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${LEFT_CF}" 2>/dev/null | jq -r ".state // empty" || true)"
  right_state="$(${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${RIGHT_CF}" 2>/dev/null | jq -r ".state // empty" || true)"
  pair="${left_state}/${right_state}"
  if [[ "${pair}" == "normal/warning" || "${pair}" == "warning/normal" ]]; then
    break
  fi
  sleep 2
done

pair="${left_state}/${right_state}"
if [[ "${pair}" != "normal/warning" && "${pair}" != "warning/normal" ]]; then
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${LEFT_CF}" || true
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${RIGHT_CF}" || true
  exit 1
fi

conflicts="$(
  for service in ticdc-1 ticdc-2 ticdc-3; do
    docker logs --since=20m "ticdc-iceberg-e2e-${service}-1" 2>/dev/null
  done | rg -c "unsupported iceberg target owner conflict|target owner conflict|owned by another changefeed" || true
)"
metric_conflicts="$(owner_conflict_metric_count)"
if [[ "${metric_conflicts}" == "0" ]]; then
  printf 'expected ticdc_sink_iceberg_target_owner_conflicts_total to be non-zero\n' >&2
  exit 1
fi
staged_during="$(stage_file_count)"

remove_feed "${LEFT_CF}"
remove_feed "${RIGHT_CF}"
trap - EXIT
sleep 5

staged_after="$(stage_file_count)"
owner_after="$(find "${WAREHOUSE}/.ticdc/iceberg-target-owners" -type f 2>/dev/null | wc -l | tr -d " ")"
if [[ "${staged_after}" != "0" || "${owner_after}" != "0" ]]; then
  printf 'staged_after_remove=%s owner_markers_after_remove=%s\n' "${staged_after}" "${owner_after}" >&2
  exit 1
fi

printf 'S15_OWNER_PASS cf_left=%s cf_right=%s summary=%s left_state=%s right_state=%s conflicts=%s metric_conflicts=%s staged_during=%s staged_after_remove=%s owner_markers_after_remove=%s\n' \
  "${LEFT_CF}" \
  "${RIGHT_CF}" \
  "${summary}" \
  "${left_state}" \
  "${right_state}" \
  "${conflicts}" \
  "${metric_conflicts}" \
  "${staged_during}" \
  "${staged_after}" \
  "${owner_after}"
