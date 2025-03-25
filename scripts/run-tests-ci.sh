#!/usr/bin/env bash
set -eou pipefail

script_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "$(dirname "${script_dir}")")

export $(cat "${project_dir}/.env.example" | xargs)

"${script_dir}/recreate-db.sh"

status=0
go test "${project_dir}/tests" || status=1

"${script_dir}/destroy-db.sh"

exit "${status}"
