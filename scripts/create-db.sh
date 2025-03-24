#!/usr/bin/env bash
set -eou pipefail

docker compose up --wait -d

project_dir=$(realpath "$(dirname "$(dirname "$0")")")
schema_dir="${project_dir}/schema"

PGPASSWORD="${POSTGRES_PASSWORD}" psql -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f "${schema_dir}/worker.sql"

