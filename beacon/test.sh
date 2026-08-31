#!/usr/bin/env bash

# ==========================================================================
# Hirebotics / Beacon — NOT part of upstream TypeORM. See beacon/README.md.
#
# Tests the Beacon patches (better-sqlite3 + postgres) plus a Postgres driver
# smoke set. Handles everything for you: starts a local Postgres in Docker,
# swaps in the Beacon ormconfig, runs the tests, then tears the container down
# and restores your previous ormconfig.json — even if a test fails or you Ctrl-C.
#
# Usage: ./beacon/test.sh   (no arguments)
#
# Prerequisites: Docker running and pnpm on PATH. Dependencies are installed automatically.
# ==========================================================================

set -euo pipefail

BEACON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$BEACON_DIR/.." && pwd)"
cd "$REPO_DIR"

command -v docker >/dev/null || { echo "[beacon] Docker is required and must be running." >&2; exit 1; }
command -v pnpm   >/dev/null || { echo "[beacon] pnpm is required." >&2; exit 1; }

echo "[beacon] installing dependencies ..."
pnpm install --frozen-lockfile

COMPOSE=(docker compose -f "$BEACON_DIR/docker-compose.yml")
PG_CONTAINER="beacon-typeorm-postgres"
BAK="$REPO_DIR/ormconfig.json.beacon-bak"

cleanup() {
    local code=$?
    echo "[beacon] tearing down Postgres ..."
    "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
    if [ -f "$BAK" ]; then
        mv -f "$BAK" "$REPO_DIR/ormconfig.json"
        echo "[beacon] restored your previous ormconfig.json"
    else
        rm -f "$REPO_DIR/ormconfig.json"
    fi
    exit "$code"
}
trap cleanup EXIT INT TERM

# 1) Swap in the Beacon ormconfig (backing up any existing local one).
[ -f "$REPO_DIR/ormconfig.json" ] && cp "$REPO_DIR/ormconfig.json" "$BAK"
cp "$BEACON_DIR/ormconfig.beacon.json" "$REPO_DIR/ormconfig.json"
echo "[beacon] using beacon/ormconfig.beacon.json (better-sqlite3 + postgres)"

# 2) Start Postgres and wait until it accepts connections.
echo "[beacon] starting Postgres ..."
"${COMPOSE[@]}" up -d

echo -n "[beacon] waiting for Postgres ..."
ready=""
for _ in $(seq 1 30); do
    if docker exec "$PG_CONTAINER" pg_isready -U test -d test >/dev/null 2>&1; then
        ready=1; break
    fi
    echo -n "."; sleep 1
done
echo
[ "$ready" = "1" ] || { echo "[beacon] Postgres did not become ready in time." >&2; exit 1; }
echo "[beacon] Postgres is ready."

# 3) Compile and run the tests: the two Beacon patch tests, plus upstream tests
#    that exercise the Postgres driver broadly (a regression check that our
#    query-runner subclass didn't break it).
echo "[beacon] compiling ..."
pnpm run compile

echo "[beacon] running Beacon tests ..."
node_modules/.bin/mocha --config "$BEACON_DIR/mocharc.beacon.json" \
    build/compiled/test/functional/driver/abstract-sqlite/sqlite-connection-lease-unit.test.js \
    build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-query-runner-ownership.test.js \
    build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-begin-immediate.test.js \
    build/compiled/test/functional/driver/abstract-sqlite/abstract-sqlite-busy-error-retry.test.js \
    build/compiled/test/functional/driver/postgres/postgres-driver-extension.test.js \
    build/compiled/test/functional/driver/postgres/connection-options.test.js \
    build/compiled/test/functional/query-builder/insert/query-builder-insert.test.js \
    build/compiled/test/functional/query-builder/update/query-builder-update.test.js \
    build/compiled/test/functional/repository/basic-methods/repository-basic-methods.test.js \
    build/compiled/test/functional/persistence/basic-functionality/persistence-basic-functionality.test.js \
    build/compiled/test/functional/transaction/return-data-from-transaction/return-data-from-transaction.test.js \
    build/compiled/test/functional/transaction/transaction-in-entity-manager/transaction-in-entity-manager.test.js \
    build/compiled/test/functional/transaction/single-query-runner/single-query-runner.test.js \
    build/compiled/test/functional/transaction/nested-transaction/transaction-in-entity-manager.test.js
