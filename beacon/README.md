# Beacon fork of TypeORM — Hirebotics

**Beacon-specific. Not upstream TypeORM.**
Everything in `beacon/` is ours, and nothing in `beacon/` edits an upstream file.

## Run the tests

```bash
./beacon/test.sh
```

[Do not run `pnpm test` directly](#do-not-run-pnpm-test-directly).

Node 20 is required, because `better-sqlite3@8.7.0` does not build on Node 22 or later.

The script runs our driver patch tests plus a Postgres and sqlite regression smoke set.
It starts Postgres in Docker, swaps in `ormconfig.beacon.json`, runs the tests,
then tears Postgres down and restores your `ormconfig.json`, even on failure or Ctrl-C.

## What we patch

| Driver                            | Change                                                                                                                                                                                   |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **sqlite** and **better-sqlite3** | A query runner per caller, leased against the one connection, so concurrent units of work cannot land in a single transaction. Plus `BEGIN IMMEDIATE` and a bounded `SQLITE_BUSY` retry. |
| **postgres**                      | `onConnect` / `onRelease` pool hooks via `extendPostgresDriver()`. Beacon uses these for per-request row-level security (`SET app.current_tenant`).                                      |

The sqlite work lives in `src/driver/sqlite-abstract/SqliteConnectionLease.ts`.
The only upstream edits are the two `createQueryRunner` bodies and the option declarations,
which keeps the rebase cost off the files upstream actively rewrites.

Covered by:

-   `test/functional/driver/abstract-sqlite/abstract-sqlite-query-runner-ownership.test.ts`
-   `test/functional/driver/abstract-sqlite/abstract-sqlite-begin-immediate.test.ts`
-   `test/functional/driver/abstract-sqlite/abstract-sqlite-busy-error-retry.test.ts`
-   `test/functional/driver/postgres/postgres-driver-extension.test.ts`

### Writing sqlite concurrency tests

A second database handle that stands in for another process
has to be opened with the **same** sqlite library as the driver under test.

Two different builds of sqlite in one process cannot block each other at all:
file locks are POSIX advisory locks, which never conflict within a process,
and sqlite's own in-process lock table is per-library.

A better-sqlite3 handle writes straight through a node-sqlite3 transaction,
so a test written that way silently measures nothing.
`sqlite-lease-test-utils.ts` handles all of that,
so use `openSecondHandle()` and do not open a handle of your own.

Avoid `timeout: 0`, which is what hid the event-loop freeze.
better-sqlite3 blocks inside C for the busy timeout on every attempt,
so a test that zeroes the timeout never sees the cost that production pays.

## Do not run `pnpm test` directly

Upstream's sample `ormconfig.json` enables the plain `sqlite` driver
with the `query` load strategy and concurrent connections.
Both settings have pre-existing upstream bugs that upstream's own test runs never exercise.
A stock `pnpm test` therefore reports about 7 failures unrelated to our patches.
`beacon/test.sh` scopes to the drivers we actually changed,
and `ormconfig.beacon.json` deliberately omits `relationLoadStrategy` so it defaults to `join`.
