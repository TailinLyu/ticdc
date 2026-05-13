#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_m6_soak_${RUN_ID}}"
CF="${CF:-m6-soak-${RUN_ID}}"
RULE="${RULE:-${DB}.orders}"
DURATION_SECONDS="${DURATION_SECONDS:-3600}"
ROWS="${ROWS:-360000}"
WORKERS="${WORKERS:-8}"
BATCH="${BATCH:-100}"
BATCH_DELAY="${BATCH_DELAY:-75ms}"
SPLIT_REGIONS="${SPLIT_REGIONS:-8}"
TIMEOUT="${TIMEOUT:-900}"
PASS_LABEL="${PASS_LABEL:-M6_SOAK_PASS}"

cleanup() {
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy
wait_stage_file_count 0 60 >/dev/null

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

started_at="$(date +%s)"
go run ./local-e2e/workload \
  --db "${DB}" \
  --tables orders \
  --rows "${ROWS}" \
  --workers "${WORKERS}" \
  --batch "${BATCH}" \
  --batch-delay "${BATCH_DELAY}" \
  --split-regions "${SPLIT_REGIONS}" >/tmp/"${DB}"-workload.json
elapsed=$(( $(date +%s) - started_at ))

expected_updates=$((ROWS / 10))
expected_deletes=$((ROWS / 15))
expected_rows=$((ROWS + expected_updates + expected_deletes))
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"
staged_after="$(wait_stage_file_count 0 180)"

sample_first=$((1 * 100))
sample_mid=$(((ROWS / 2) * 100))
sample_last=$((ROWS * 100))
sample_ids="${sample_first},${sample_mid},${sample_last}"
sample_check="$(go run ./local-e2e/icebergread \
  --warehouse file:///tmp/iceberg-warehouse \
  --catalog http://127.0.0.1:8181 \
  --namespace "${DB}" \
  --table orders_cdc \
  --require-insert-ids "${sample_ids}")"

lease_prefix="/tidb/cdc/default/__cdc_meta__/iceberg-committer/"
lease_lines="$(${COMPOSE} exec -T pd sh -c "ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 get --prefix ${lease_prefix}" 2>/dev/null | wc -l | tr -d ' ' || true)"
if [[ -z "${lease_lines}" || "${lease_lines}" == "0" ]]; then
  printf 'expected etcd committer lease under %s\n' "${lease_prefix}" >&2
  exit 1
fi

if (( elapsed < DURATION_SECONDS )); then
  printf 'warning: workload finished in %ss before requested soak duration %ss; raise ROWS or BATCH_DELAY for a strict 1h run\n' \
    "${elapsed}" "${DURATION_SECONDS}" >&2
fi

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 120)"

printf '%s cf=%s rows=%s workers=%s batch=%s batch_delay=%s elapsed_seconds=%s summary=%s staged_after=%s staged_after_remove=%s invariant_sample=%q lease_lines=%s\n' \
  "${PASS_LABEL}" "${CF}" "${ROWS}" "${WORKERS}" "${BATCH}" "${BATCH_DELAY}" "${elapsed}" "${summary}" "${staged_after}" "${staged_after_remove}" "${sample_check}" "${lease_lines}"
