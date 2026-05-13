#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPOSE="${COMPOSE:-docker compose -f ${ROOT_DIR}/local-e2e/docker-compose.yml}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-ticdc-iceberg-owner}"
MINIO_PREFIX="${MINIO_PREFIX:-s16-owner-marker-$(date +%s)}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9000/}"
MINIO_NETWORK="${MINIO_NETWORK:-ticdc-iceberg-e2e_default}"
MC_IMAGE="${MC_IMAGE:-minio/mc:latest}"

${COMPOSE} up -d minio >/dev/null

for _ in $(seq 1 60); do
  if curl -fsS "${MINIO_ENDPOINT}minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "${MINIO_ENDPOINT}minio/health/ready" >/dev/null

docker run --rm --network "${MINIO_NETWORK}" --entrypoint /bin/sh "${MC_IMAGE}" -c \
  "mc alias set local http://minio:9000 '${MINIO_USER}' '${MINIO_PASSWORD}' >/dev/null && \
   mc mb --ignore-existing local/'${MINIO_BUCKET}' >/dev/null && \
   mc rm --recursive --force local/'${MINIO_BUCKET}'/'${MINIO_PREFIX}' >/dev/null 2>&1 || true"

warehouse="s3://${MINIO_BUCKET}/${MINIO_PREFIX}?endpoint=${MINIO_ENDPOINT}&access-key=${MINIO_USER}&secret-access-key=${MINIO_PASSWORD}&force-path-style=true"
ICEBERG_MINIO_WAREHOUSE="${warehouse}" go test ./pkg/sink/iceberg \
  -run TestClaimTargetOwnerAgainstMinIO -count=1 -v

docker run --rm --network "${MINIO_NETWORK}" --entrypoint /bin/sh "${MC_IMAGE}" -c \
  "mc alias set local http://minio:9000 '${MINIO_USER}' '${MINIO_PASSWORD}' >/dev/null && \
   mc rm --recursive --force local/'${MINIO_BUCKET}'/'${MINIO_PREFIX}' >/dev/null 2>&1 || true"

printf 'S16_MINIO_OWNER_PASS bucket=%s prefix=%s\n' "${MINIO_BUCKET}" "${MINIO_PREFIX}"
