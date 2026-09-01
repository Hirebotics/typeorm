#!/bin/bash

set -e

# Hirebotics / Beacon — NOT part of upstream TypeORM. See beacon/README.md.

# Resolve the directories before any cd, because BASH_SOURCE is relative to the caller's cwd.
BEACON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd "${BEACON_DIR}" > /dev/null 2>&1 && cd .. && pwd)"

# Run from the repo root so relative paths and pnpm scripts resolve from any cwd.
cd "${ROOT_DIR}"

usage() {
  echo "Description:" 1>&2
  echo "  Tests the Beacon patches to the sqlite and postgres drivers, plus an" 1>&2
  echo "  upstream smoke set that checks the patches broke nothing else." 1>&2
  echo "  Starts a local Postgres in Docker and swaps in the Beacon ormconfig," 1>&2
  echo "  then restores your ormconfig and tears the container down on exit," 1>&2
  echo "  even if a test fails or you Ctrl-C." 1>&2
  echo "" 1>&2
  echo "Usage:" 1>&2
  echo "  $0 [OPTIONS]" 1>&2
  echo "" 1>&2
  echo "Options:" 1>&2
  echo "  -c, --coverage            Measure coverage of the fork-owned driver files and" 1>&2
  echo "                            write text, html, and lcov reports to beacon/coverage." 1>&2
  echo "  -h, --help                Show this menu." 1>&2
  echo "" 1>&2
  echo "Requires:" 1>&2
  echo "  Docker running, pnpm on PATH, and node 20." 1>&2
  echo "" 1>&2
  exit 1
}

while [ "$1" != "" ]; do
  case $1 in
    -c | --coverage)
      COVERAGE="true"
      ;;
    -h | --help)
      usage
      ;;
    *)
      echo "unknown option '$1'" 1>&2
      usage
      ;;
  esac
  if [ "$#" -gt 0 ]; then
    shift
  fi
done

# Assign values in order of precedence: flags > env vars > defaults
COVERAGE="${COVERAGE:-"false"}"

# Validate the environment before doing any work.
if [ -z "$(command -v docker)" ]; then
  echo "docker is required and must be running" 1>&2
  exit 1
fi

if [ -z "$(command -v pnpm)" ]; then
  echo "pnpm is required" 1>&2
  exit 1
fi

# Read the required major from .nvmrc so this check cannot drift from it.
# better-sqlite3@8.7.0 has no prebuilt binary for node 22+ and fails to build,
# and the failure surfaces as a confusing native module error much later.
REQUIRED_NODE_MAJOR="$(tr -d 'v[:space:]' < "${ROOT_DIR}/.nvmrc" | cut -d. -f1)"
ACTUAL_NODE_MAJOR="$(node --version | tr -d 'v' | cut -d. -f1)"

if [ "${ACTUAL_NODE_MAJOR}" != "${REQUIRED_NODE_MAJOR}" ]; then
  echo "node ${REQUIRED_NODE_MAJOR} is required, found $(node --version)" 1>&2
  echo "run 'nvm use' to pick up .nvmrc" 1>&2
  exit 1
fi

COMPOSE=(docker compose -f "${BEACON_DIR}/docker-compose.yml")
PG_CONTAINER="beacon-typeorm-postgres"
ORMCONFIG="${ROOT_DIR}/ormconfig.json"
ORMCONFIG_BACKUP="${ROOT_DIR}/ormconfig.json.beacon-bak"
COVERAGE_DIR="${BEACON_DIR}/coverage"

# The tests the Beacon patches own, plus upstream suites that exercise the
# patched drivers broadly enough to catch a regression.
TEST_FILES=(
  build/compiled/test/functional/driver/abstract-sqlite/sqlite-connection-serializer-unit.test.js
  build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-query-runner-ownership.test.js
  build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-begin-immediate.test.js
  build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-busy-error-retry.test.js
  build/compiled/test/functional/driver/postgres/postgres-driver-extension.test.js
  build/compiled/test/functional/driver/postgres/connection-options.test.js
  build/compiled/test/functional/query-builder/insert/query-builder-insert.test.js
  build/compiled/test/functional/query-builder/update/query-builder-update.test.js
  build/compiled/test/functional/repository/basic-methods/repository-basic-methods.test.js
  build/compiled/test/functional/persistence/basic-functionality/persistence-basic-functionality.test.js
  build/compiled/test/functional/transaction/return-data-from-transaction/return-data-from-transaction.test.js
  build/compiled/test/functional/transaction/transaction-in-entity-manager/transaction-in-entity-manager.test.js
  build/compiled/test/functional/transaction/single-query-runner/single-query-runner.test.js
  build/compiled/test/functional/transaction/nested-transaction/transaction-in-entity-manager.test.js
)

# c8 maps compiled output back through source maps, so these name the
# TypeScript sources rather than the files mocha loads.
COVERED_FILES=(
  src/driver/sqlite-abstract/SqliteConnectionSerializer.ts
  src/driver/sqlite-abstract/SqliteConnectionLease.ts
  src/driver/sqlite/SerializedSqliteQueryRunner.ts
  src/driver/better-sqlite3/SerializedBetterSqlite3QueryRunner.ts
  src/driver/postgres/PostgresDriverExtension.ts
)

# Restores the developer's ormconfig and stops Postgres, however we exit.
cleanup() {
  local code=$?

  echo "tearing down postgres"
  "${COMPOSE[@]}" down -v > /dev/null 2>&1 || true

  if [ -f "${ORMCONFIG_BACKUP}" ]; then
    mv -f "${ORMCONFIG_BACKUP}" "${ORMCONFIG}"
    echo "restored your previous ormconfig.json"
  else
    rm -f "${ORMCONFIG}"
  fi

  exit "${code}"
}
trap cleanup EXIT INT TERM

# Builds the c8 command prefix, or nothing when coverage is off.
build_coverage_command() {
  if [ "${COVERAGE}" != "true" ]; then
    return
  fi

  local includes=""
  for file in "${COVERED_FILES[@]}"; do
    includes="${includes} --include=${file}"
  done

  echo "node_modules/.bin/c8" \
    "--reporter=text" \
    "--reporter=html" \
    "--reporter=lcov" \
    "--temp-directory=${COVERAGE_DIR}/.tmp" \
    "--report-dir=${COVERAGE_DIR}" \
    "${includes}"
}

install_dependencies() {
  echo "installing dependencies"
  pnpm install --frozen-lockfile
}

# Swaps in the Beacon ormconfig, keeping the developer's own for `cleanup` to restore.
use_beacon_ormconfig() {
  echo "using beacon/ormconfig.beacon.json (sqlite, better-sqlite3, postgres)"

  if [ -f "${ORMCONFIG}" ]; then
    cp "${ORMCONFIG}" "${ORMCONFIG_BACKUP}"
  fi

  cp "${BEACON_DIR}/ormconfig.beacon.json" "${ORMCONFIG}"
}

# Starts Postgres and blocks until it accepts connections.
start_postgres() {
  echo "starting postgres"
  "${COMPOSE[@]}" up -d

  echo -n "waiting for postgres"
  for _ in $(seq 1 30); do
    if docker exec "${PG_CONTAINER}" pg_isready -U test -d test > /dev/null 2>&1; then
      echo ""
      return
    fi
    echo -n "."
    sleep 1
  done
  echo ""

  echo "postgres did not become ready in time" 1>&2
  exit 1
}

compile_typeorm() {
  echo "compiling"
  pnpm run compile
}

run_tests() {
  if [ "${COVERAGE}" == "true" ]; then
    rm -rf "${COVERAGE_DIR}"
  fi

  echo "running beacon tests: coverage=${COVERAGE}"

  # Declared and assigned separately so `local` cannot mask a failure.
  local coverage_command
  coverage_command="$(build_coverage_command)"

  # Unquoted on purpose: empty when coverage is off, and c8's flags must split.
  ${coverage_command} node_modules/.bin/mocha \
    --config "${BEACON_DIR}/mocharc.beacon.json" \
    "${TEST_FILES[@]}"

  if [ "${COVERAGE}" == "true" ]; then
    rm -rf "${COVERAGE_DIR}/.tmp"
    echo "coverage report: beacon/coverage/index.html"
  fi
}

install_dependencies
use_beacon_ormconfig
start_postgres
compile_typeorm
run_tests

echo "beacon tests passed"
