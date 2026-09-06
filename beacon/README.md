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

| Driver                            | Change                                                                                                                                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **sqlite** and **better-sqlite3** | A query runner per caller, each holding the one connection from its first statement until `release()`, so concurrent units of work cannot land in a single transaction. Plus `BEGIN IMMEDIATE`. |
| **postgres**                      | `onConnect` / `onRelease` pool hooks via `extendPostgresDriver()`. Beacon uses these for per-request row-level security (`SET app.current_tenant`).                                             |

The sqlite work is one fork-owned file, `src/driver/sqlite-abstract/SqliteConnectionLock.ts`,
plus edits to four upstream files. Every edit carries a `Hirebotics patch:` comment.

### Why there is a lock at all

sqlite supports many connections to one file; TypeORM's sqlite driver just holds one.
The connection stays single because **each connection to `:memory:` is a separate database**,
and consumers run `:memory:` in tests. That is what makes the lock necessary, not any limit in sqlite.

sqlite's own busy handler cannot do this job. Two runners on one connection do not contend for a
file lock — the second `BEGIN` fails `SQLITE_ERROR`, not `SQLITE_BUSY` — and a busy wait would
block the event loop that the holder needs in order to reach its `COMMIT`.

### Why there is no retry code

`BEGIN IMMEDIATE` is what fixes the `SQLITE_BUSY` alerts, not retrying.

sqlite's busy handler is **never invoked** for `SQLITE_BUSY_SNAPSHOT`, the error a deferred
`BEGIN` hits when another connection wrote between its read snapshot and its first write.
Measured identical at `busy_timeout` 0, 1000 and 5000: it fails instantly at every value.
Retrying it can never succeed either — 10 of 10 attempts failed against an otherwise idle
database, because the connection's snapshot is permanently stale.

That is why BEACON-1491's retry fix did not stop the alerts, and why BEACON-1684 saw a
multi-second freeze: ten retries of an error that cannot clear.

Once `BEGIN IMMEDIATE` removes that error class and the lock removes same-process contention,
the only `SQLITE_BUSY` left comes from PowerSync's own connection, and sqlite's `busy_timeout`
handles that correctly in C.

The fork exposes one option, `connectionLeaseTimeout`, to tune the acquire deadline.

Covered by:

-   `test/functional/driver/abstract-sqlite/abstract-sqlite-query-runner-ownership.test.ts`
-   `test/functional/driver/abstract-sqlite/abstract-sqlite-begin-immediate.test.ts`
-   `test/functional/driver/abstract-sqlite/abstract-sqlite-escape-query-parameters.test.ts`
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

A concurrency test must assert against a contended lock, not an idle one.
A double release is harmless when nobody is queued, so queue the waiters **before**
the release you are testing, or the test passes with the code removed.

## Do not run `pnpm test` directly

Upstream's sample `ormconfig.json` enables the plain `sqlite` driver
with the `query` load strategy and concurrent connections.
Both settings have pre-existing upstream bugs that upstream's own test runs never exercise.
A stock `pnpm test` therefore reports about 7 failures unrelated to our patches.
`beacon/test.sh` scopes to the drivers we actually changed,
and `ormconfig.beacon.json` deliberately omits `relationLoadStrategy` so it defaults to `join`.
