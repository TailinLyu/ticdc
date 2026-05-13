#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
STAGING_DIR="${STAGING_DIR:-file:///data/ticdc/.iceberg-staging}"
source "${ROOT_DIR}/local-e2e/iceberg_case_lib.sh"

ROWS="${ROWS:-400}"
WORKERS="${WORKERS:-4}"
BATCH="${BATCH:-25}"
SPLIT_REGIONS="${SPLIT_REGIONS:-6}"
TIMEOUT="${TIMEOUT:-240}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s28_lease_${RUN_ID}}"
CF="${CF:-s28-lease-${RUN_ID}}"

cleanup() {
  remove_feed "${CF}" || true
}
trap cleanup EXIT

wait_captures 3 120
ensure_stack_healthy
wait_stage_file_count 0 30 >/dev/null

since="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

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
staged_after="$(wait_stage_file_count 0 "${STAGE_TIMEOUT:-120}")"
summary="$(wait_readback "${DB}" orders_cdc "${expected_rows}" "${ROWS}" "${expected_updates}" "${expected_deletes}" "${TIMEOUT}")"

writer_services="$(log_services_for "${since}" "${CF}" "iceberg writer staged rows")"
non_committer_writer_services="$(log_services_matching "${since}" "iceberg writer staged rows.*${CF}.*committer=false")"
append_services="$(log_services_for "${since}" "${CF}" "iceberg committer appended staged rows")"
non_committer_append_services="$(log_services_matching "${since}" "iceberg committer appended staged rows.*${CF}.*committer=false")"
enabled_services="$(log_services_for "${since}" "${CF}" "iceberg committer enabled")"
lease_services="$(log_services_for "${since}" "${CF}" "iceberg target committer lease acquired")"
release_services="$(log_services_for "${since}" "${CF}" "iceberg target committer lease released")"
lease_conflicts="$(log_services_matching "${since}" "etcd committer lease owner.*${CF}.*conflicts")"

if [[ -z "${non_committer_writer_services}" ]]; then
  printf 'expected at least one non-committer data dispatcher to stage rows\n' >&2
  printf 'writer_services=%s append_services=%s non_committer_append_services=%s enabled_services=%s lease_services=%s release_services=%s lease_conflicts=%s\n' \
    "${writer_services}" "${append_services}" "${non_committer_append_services}" "${enabled_services}" "${lease_services}" "${release_services}" "${lease_conflicts}" >&2
  exit 1
fi

if [[ -z "${append_services}" ]]; then
  printf 'expected a committer to append staged rows\n' >&2
  printf 'writer_services=%s non_committer_writer_services=%s non_committer_append_services=%s enabled_services=%s lease_services=%s release_services=%s lease_conflicts=%s\n' \
    "${writer_services}" "${non_committer_writer_services}" "${non_committer_append_services}" "${enabled_services}" "${lease_services}" "${release_services}" "${lease_conflicts}" >&2
  exit 1
fi

if [[ -z "${non_committer_append_services}" ]]; then
  printf 'expected at least one non-committer data dispatcher to acquire the lease and append its local staged rows\n' >&2
  printf 'writer_services=%s non_committer_writer_services=%s append_services=%s enabled_services=%s lease_services=%s release_services=%s lease_conflicts=%s\n' \
    "${writer_services}" "${non_committer_writer_services}" "${append_services}" "${enabled_services}" "${lease_services}" "${release_services}" "${lease_conflicts}" >&2
  exit 1
fi

if [[ -z "${lease_services}" || -z "${release_services}" ]]; then
  printf 'expected target committer lease acquisition and release logs\n' >&2
  printf 'writer_services=%s non_committer_writer_services=%s append_services=%s non_committer_append_services=%s enabled_services=%s lease_services=%s release_services=%s lease_conflicts=%s\n' \
    "${writer_services}" "${non_committer_writer_services}" "${append_services}" "${non_committer_append_services}" "${enabled_services}" "${lease_services}" "${release_services}" "${lease_conflicts}" >&2
  exit 1
fi

if [[ -n "${lease_conflicts}" ]]; then
  printf 'unexpected etcd committer lease conflict for same changefeed\n' >&2
  printf 'writer_services=%s non_committer_writer_services=%s append_services=%s non_committer_append_services=%s enabled_services=%s lease_services=%s release_services=%s lease_conflicts=%s\n' \
    "${writer_services}" "${non_committer_writer_services}" "${append_services}" "${non_committer_append_services}" "${enabled_services}" "${lease_services}" "${release_services}" "${lease_conflicts}" >&2
  exit 1
fi

remove_feed "${CF}"
trap - EXIT
sleep 5
staged_after_remove="$(wait_stage_file_count 0 60)"

printf 'S28_LEASE_DRAIN_PASS cf=%s summary=%s staged_after=%s staged_after_remove=%s staging_dir=%s writer_services=%s non_committer_writer_services=%s append_services=%s non_committer_append_services=%s enabled_services=%s lease_services=%s release_services=%s\n' \
  "${CF}" \
  "${summary}" \
  "${staged_after}" \
  "${staged_after_remove}" \
  "${STAGING_DIR}" \
  "${writer_services}" \
  "${non_committer_writer_services}" \
  "${append_services}" \
  "${non_committer_append_services}" \
  "${enabled_services}" \
  "${lease_services}" \
  "${release_services}"
