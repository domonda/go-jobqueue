#!/usr/bin/env bash
set -eou pipefail

script_dir=$(realpath "$(dirname "$0")")

"${script_dir}/destroy-db.sh"
"${script_dir}/create-db.sh"
