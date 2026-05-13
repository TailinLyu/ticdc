#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPOSE="${COMPOSE:-docker compose -f ${ROOT_DIR}/local-e2e/docker-compose.yml}"
WAREHOUSE="${WAREHOUSE:-/tmp/iceberg-warehouse}"
STAGING_DIR="${STAGING_DIR:-file:///tmp/iceberg-warehouse/.ticdc-staging}"
SINK_BASE="${SINK_BASE:-iceberg://iceberg-rest:8181/?warehouse=file:///tmp/iceberg-warehouse&commit-interval=1s&batch-rows=${BATCH_ROWS:-25}&staging-dir=${STAGING_DIR}}"

server_for_service() {
  case "$1" in
    ticdc-1) printf 'http://127.0.0.1:8300' ;;
    ticdc-2) printf 'http://127.0.0.1:8301' ;;
    ticdc-3) printf 'http://127.0.0.1:8302' ;;
    *) printf '%s' "$1" ;;
  esac
}

create_changefeed_config() {
  local cf="$1"
  local rule="$2"
  local service="${3:-ticdc-1}"
  local config="/tmp/${cf}.toml"
  cat >"${config}" <<EOF
[scheduler]
enable-table-across-nodes = true
region-threshold = 1
region-count-per-span = 1
force-split = true

[filter]
rules = ['${rule}']
EOF
  ${COMPOSE} cp "${config}" "${service}:${config}" >/dev/null
}

create_feed() {
  local cf="$1"
  local rule="$2"
  local suffix="$3"
  local server="${4:-ticdc-1}"
  local extra="${5:-}"
  local service="${server}"
  case "${service}" in
    ticdc-*) ;;
    *) service="ticdc-1" ;;
  esac
  create_changefeed_config "${cf}" "${rule}" "${service}"
  local uri="${SINK_BASE}&table-suffix=${suffix}${extra}"
  local cli_server
  if [[ "${service}" == "${server}" && "${service}" == ticdc-* ]]; then
    cli_server="http://127.0.0.1:8300"
  else
    cli_server="$(server_for_service "${server}")"
  fi
  local out=""
  for _ in $(seq 1 30); do
    if out="$(${COMPOSE} exec -T "${service}" /cdc cli changefeed create \
      --server="${cli_server}" \
      --changefeed-id "${cf}" \
      --sink-uri "${uri}" \
      --config "/tmp/${cf}.toml" \
      --no-confirm 2>&1)"; then
      return 0
    fi
    sleep 2
  done
  printf '%s\n' "${out}" >&2
  return 1
}

remove_feed() {
  local cf="$1"
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed remove \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${cf}" >/dev/null 2>&1 || true
}

wait_captures() {
  local expected="${1:-3}"
  local deadline=$((SECONDS + ${2:-90}))
  while (( SECONDS < deadline )); do
    local count
    count="$(${COMPOSE} exec -T ticdc-1 /cdc cli capture list --server=http://127.0.0.1:8300 2>/dev/null | jq 'length' 2>/dev/null || true)"
    if [[ "${count}" == "${expected}" ]]; then
      return 0
    fi
    sleep 2
  done
  ${COMPOSE} exec -T ticdc-1 /cdc cli capture list --server=http://127.0.0.1:8300
  return 1
}

wait_feed_normal() {
  local cf="$1"
  local deadline=$((SECONDS + ${2:-120}))
  while (( SECONDS < deadline )); do
    local state
    state="$(${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
      --server=http://127.0.0.1:8300 \
      --changefeed-id "${cf}" 2>/dev/null | jq -r '.state // empty' || true)"
    if [[ "${state}" == "normal" ]]; then
      return 0
    fi
    sleep 2
  done
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query --server=http://127.0.0.1:8300 --changefeed-id "${cf}" || true
  return 1
}

changefeed_checkpoint_tso() {
  local cf="$1"
  ${COMPOSE} exec -T ticdc-1 /cdc cli changefeed query \
    --server=http://127.0.0.1:8300 \
    --changefeed-id "${cf}" 2>/dev/null |
    jq -r '.checkpoint_tso // 0'
}

readback_summary() {
  local namespace="$1"
  local table="$2"
  go run ./local-e2e/icebergread \
    --summary \
    --warehouse file:///tmp/iceberg-warehouse \
    --catalog http://127.0.0.1:8181 \
    --namespace "${namespace}" \
    --table "${table}"
}

wait_readback() {
  local namespace="$1"
  local table="$2"
  local rows="$3"
  local inserts="$4"
  local updates="$5"
  local deletes="$6"
  local deadline=$((SECONDS + ${7:-180}))
  local want="rows=${rows} inserts=${inserts} updates=${updates} deletes=${deletes}"
  local out=""
  while (( SECONDS < deadline )); do
    out="$(readback_summary "${namespace}" "${table}" 2>&1 || true)"
    if [[ "${out}" == "${want}" ]]; then
      printf '%s\n' "${out}"
      return 0
    fi
    sleep 3
  done
  printf '%s\n' "${out}"
  return 1
}

stage_file_count() {
  local staging_path="${STAGING_DIR#file://}"
  if [[ "${STAGING_DIR}" != file://* || "${staging_path}" == "${WAREHOUSE}"* ]]; then
    { find "${staging_path}" -name '*.json' 2>/dev/null || true; } | wc -l | tr -d ' '
    return
  fi

  local total=0
  local count
  for service in ticdc-1 ticdc-2 ticdc-3; do
    count="$(${COMPOSE} exec -T "${service}" /bin/sh -c "find '${staging_path}' -name '*.json' 2>/dev/null | wc -l" 2>/dev/null || printf '0')"
    count="$(printf '%s' "${count}" | tr -d ' ')"
    total=$((total + count))
  done
  printf '%s\n' "${total}"
}

oldest_staged_max_commit_ts() {
  local ts
  ts="$(
    { find "${WAREHOUSE}/.ticdc-staging" -name '*.json' -type f -print0 2>/dev/null || true; } |
      xargs -0 jq -r '.max_commit_ts // empty' 2>/dev/null |
      sort -n |
      head -n 1
  )"
  printf '%s\n' "${ts:-0}"
}

wait_stage_file_count() {
  local expected="$1"
  local timeout="${2:-120}"
  local deadline=$((SECONDS + timeout))
  local count
  while (( SECONDS < deadline )); do
    count="$(stage_file_count)"
    if [[ "${count}" == "${expected}" ]]; then
      printf '%s\n' "${count}"
      return 0
    fi
    sleep 2
  done
  count="$(stage_file_count)"
  if [[ "${count}" == "${expected}" ]]; then
    printf '%s\n' "${count}"
    return 0
  fi
  printf 'expected staged file count %s, got %s\n' "${expected}" "${count}" >&2
  return 1
}

wait_stage_file_count_at_least() {
  local minimum="$1"
  local timeout="${2:-120}"
  local deadline=$((SECONDS + timeout))
  local count
  while (( SECONDS < deadline )); do
    count="$(stage_file_count)"
    if (( count >= minimum )); then
      printf '%s\n' "${count}"
      return 0
    fi
    sleep 2
  done
  count="$(stage_file_count)"
  if (( count >= minimum )); then
    printf '%s\n' "${count}"
    return 0
  fi
  printf 'expected at least %s staged files, got %s\n' "${minimum}" "${count}" >&2
  return 1
}

wait_iceberg_rest() {
  local timeout="${1:-120}"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if curl -sf http://127.0.0.1:8181/v1/config >/dev/null; then
      return 0
    fi
    sleep 2
  done
  curl -sv http://127.0.0.1:8181/v1/config >/dev/null || true
  return 1
}

table_id() {
  local db="$1"
  local table="$2"
  go run ./local-e2e/tidbexec -query \
    "SELECT tidb_table_id FROM information_schema.tables WHERE table_schema='${db}' AND table_name='${table}'" |
    awk 'NR==2 {print $1}'
}

region_line_count() {
  local db="$1"
  local table="$2"
  go run ./local-e2e/tidbexec -query "SHOW TABLE ${db}.${table} REGIONS" | tail -n +2 | wc -l | tr -d ' '
}

log_services_matching() {
  local since="$1"
  local pattern="$2"
  local matches
  matches="$(
    for service in ticdc-1 ticdc-2 ticdc-3; do
      docker logs --since="${since}" "ticdc-iceberg-e2e-${service}-1" 2>/dev/null |
        sed "s/^/${service}-1  | /"
    done |
      rg "${pattern}" || true
  )"
  if [[ -z "${matches}" ]]; then
    return 0
  fi
  printf '%s\n' "${matches}" |
    awk -F'  \\| ' '{print $1}' |
    sort | uniq -c |
    awk '{print $2 "=" $1}' |
    paste -sd, -
}

log_services_for() {
  local since="$1"
  local cf="$2"
  local pattern="$3"
  log_services_matching "${since}" "${pattern}.*${cf}"
}

ensure_stack_healthy() {
  wait_captures 3 90
  local out=""
  for _ in $(seq 1 60); do
    if out="$(go run ./local-e2e/workload --db ticdc_health_probe --tables t --rows 2 --workers 1 --batch 2 --split-regions 0 --reset 2>&1)"; then
      return 0
    fi
    sleep 2
  done
  printf '%s\n' "${out}" >&2
  return 1
}

iceberg_failpoint() {
  printf 'github.com/pingcap/ticdc/downstreamadapter/sink/iceberg/%s' "$1"
}

set_all_ticdc_failpoints() {
  local expr="$1"
  env \
    TICDC_1_GO_FAILPOINTS="${expr}" \
    TICDC_2_GO_FAILPOINTS="${expr}" \
    TICDC_3_GO_FAILPOINTS="${expr}" \
    ${COMPOSE} up -d --force-recreate --no-deps ticdc-1 ticdc-2 ticdc-3 >/dev/null
}

clear_all_ticdc_failpoints() {
  env \
    TICDC_1_GO_FAILPOINTS= \
    TICDC_2_GO_FAILPOINTS= \
    TICDC_3_GO_FAILPOINTS= \
    ${COMPOSE} up -d --force-recreate --no-deps ticdc-1 ticdc-2 ticdc-3 >/dev/null
  wait_captures 3 120
}

wait_failpoint_log() {
  local failpoint="$1"
  local deadline=$((SECONDS + ${2:-120}))
  while ((SECONDS < deadline)); do
    for service in ticdc-1 ticdc-2 ticdc-3; do
      if docker logs --since=20m "ticdc-iceberg-e2e-${service}-1" 2>/dev/null |
        rg -q "inject ${failpoint}"; then
        return 0
      fi
    done
    sleep 2
  done
  for service in ticdc-1 ticdc-2 ticdc-3; do
    docker logs --since=20m "ticdc-iceberg-e2e-${service}-1" 2>/dev/null |
      rg "IcebergSink|iceberg committer|iceberg writer" || true
  done
  return 1
}
