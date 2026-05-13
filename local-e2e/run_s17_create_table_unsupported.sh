#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
RUN_ID="${RUN_ID:-$(date +%s)}"
DB="${DB:-ice_s17_create_${RUN_ID}}"

export RUN_ID
export DB
export CF="${CF:-s17-create-unsupported-${RUN_ID}}"
export RULE="${RULE:-${DB}.*}"
export DDL_SQL="${DDL_SQL:-CREATE TABLE ${DB}.orders_created (id BIGINT PRIMARY KEY, note VARCHAR(32))}"
export PASS_LABEL="${PASS_LABEL:-S17_CREATE_TABLE_UNSUPPORTED_PASS}"

exec "${ROOT_DIR}/local-e2e/run_s17_schema_unsupported.sh"
