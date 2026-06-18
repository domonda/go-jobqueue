#!/usr/bin/env bash
set -eou pipefail

script_dir=$(realpath "$(dirname "$0")")
project_dir=$(realpath "$(dirname "${script_dir}")")

# Load environment from .env.example
export $(cat "${project_dir}/.env.example" | xargs)

usage() {
    echo "Usage: $0 [options] [-- go-test-flags]"
    echo ""
    echo "Options:"
    echo "  -v          Verbose test output"
    echo "  -d          Destroy docker compose after tests"
    echo "  -s          Skip static analysis (go vet, revive, gosec)"
    echo "  -h          Show this help"
    echo ""
    echo "Before the tests, the script runs static analysis (go vet, revive,"
    echo "gosec) unless -s is given. revive and gosec are pinned in tools/go.mod"
    echo "and run via 'go tool -modfile=tools/go.mod ...'."
    echo ""
    echo "Each test run creates a temporary database (test-jobqueue-XXXXXXXX),"
    echo "applies the schema, runs the tests, and drops the database."
    echo ""
    echo "The script checks if a PostgreSQL instance is reachable at"
    echo "POSTGRES_HOST:POSTGRES_PORT (from .env.example, default 127.0.0.1:5432)"
    echo "with user POSTGRES_USER (default 'postgres'). The user must have"
    echo "CREATE DATABASE privileges."
    echo ""
    echo "If no PostgreSQL instance is reachable, one is started via docker compose."
    echo ""
    echo "Examples:"
    echo "  $0                    # Lint, then run all tests"
    echo "  $0 -v                 # Verbose test output"
    echo "  $0 -d                 # Destroy docker compose after tests"
    echo "  $0 -s                 # Skip static analysis, run tests only"
    echo "  $0 -- -run TestReset  # Run only tests matching 'TestReset'"
    exit 0
}

verbose=""
destroy_after=false
skip_lint=false

while getopts "vdsh" opt; do
    case "${opt}" in
        v) verbose="-v" ;;
        d) destroy_after=true ;;
        s) skip_lint=true ;;
        h) usage ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

# Consume optional "--" separator for extra go test flags
if [[ "${1:-}" == "--" ]]; then
    shift
fi
extra_flags=("$@")

cd "${project_dir}"

# Static analysis: go vet, revive, gosec.
# Runs before the database setup so lint failures are reported fast without
# spinning up PostgreSQL. revive and gosec are pinned as tool dependencies in
# tools/go.mod (a separate module, so they stay out of this module's dependency
# graph for importers) and invoked via "go tool -modfile=tools/go.mod".
# revive is configured (revive.toml) with warningCode=0/errorCode=0, so it only
# reports and never fails the run, matching the domonda-service convention.
if ! ${skip_lint}; then
    echo "==> Vetting (go vet)"
    go vet ./...

    echo "==> Linting (revive)"
    go tool -modfile=tools/go.mod revive -config revive.toml -formatter friendly ./...

    echo "==> Security scanning (gosec)"
    go tool -modfile=tools/go.mod gosec -quiet ./...
fi

# Connection parameters
pg_host="${POSTGRES_HOST:-127.0.0.1}"
pg_port="${POSTGRES_PORT:-5432}"
pg_user="${POSTGRES_USER:-postgres}"
pg_password="${POSTGRES_PASSWORD:-postgres}"

export PGPASSWORD="${pg_password}"

started_docker=false

# Check if postgres is already reachable with the configured credentials,
# otherwise start via docker compose.
if psql -h "${pg_host}" -p "${pg_port}" -U "${pg_user}" -d postgres -c "SELECT 1" --quiet >/dev/null 2>&1; then
    echo "==> Using existing PostgreSQL at ${pg_host}:${pg_port} (user: ${pg_user})"
elif pg_isready -h "${pg_host}" -p "${pg_port}" -q 2>/dev/null; then
    echo "ERROR: PostgreSQL is running at ${pg_host}:${pg_port} but cannot authenticate as user '${pg_user}'."
    echo ""
    echo "The test script needs a PostgreSQL user with CREATE DATABASE privileges."
    echo "Options:"
    echo "  1. Create the user:  CREATE ROLE ${pg_user} LOGIN SUPERUSER PASSWORD '${pg_password}';"
    echo "  2. Stop the existing PostgreSQL so docker compose can start a fresh instance."
    echo "  3. Set POSTGRES_USER and POSTGRES_PASSWORD to match your existing instance."
    exit 1
else
    echo "==> No PostgreSQL found at ${pg_host}:${pg_port}, starting via docker compose..."
    docker compose up --wait -d
    started_docker=true
    echo "==> Docker compose PostgreSQL is ready"
fi

# Generate random database name
db_name="test-jobqueue-$(head -c4 /dev/urandom | xxd -p)"
echo "==> Creating temporary database: ${db_name}"

# Cleanup function: always drop the temp database
cleanup() {
    echo "==> Dropping temporary database: ${db_name}"
    psql -h "${pg_host}" -p "${pg_port}" -U "${pg_user}" -d postgres \
        -c "DROP DATABASE IF EXISTS \"${db_name}\"" --quiet 2>/dev/null || true

    if ${destroy_after} && ${started_docker}; then
        echo "==> Destroying docker compose..."
        docker compose down -v
    fi
}
trap cleanup EXIT

# Create the temporary database and apply schema
psql -h "${pg_host}" -p "${pg_port}" -U "${pg_user}" -d postgres \
    -c "CREATE DATABASE \"${db_name}\"" --quiet
echo "==> Applying schema from schema/worker.sql"
psql -h "${pg_host}" -p "${pg_port}" -U "${pg_user}" -d "${db_name}" \
    -f "${project_dir}/schema/worker.sql" --quiet

# Run tests with the temporary database
echo "==> Running tests (POSTGRES_DB=${db_name})..."
export POSTGRES_DB="${db_name}"

status=0
go test ${verbose} -count 1 -timeout 120s "${extra_flags[@]+"${extra_flags[@]}"}" ./... || status=1

if [[ ${status} -eq 0 ]]; then
    echo "==> All tests passed"
else
    echo "==> Tests FAILED"
fi

exit "${status}"
