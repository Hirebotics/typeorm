# Beacon fork of TypeORM — Hirebotics

**Beacon-specific. Not upstream TypeORM.** Everything in `beacon/` is ours; nothing here edits an upstream file.

## Run the tests

```bash
./beacon/test.sh
```

Runs our two driver patch tests plus a Postgres driver regression smoke set.
It starts Postgres in Docker, swaps in `ormconfig.beacon.json`, runs the tests,
then tears Postgres down and restores your `ormconfig.json` — even on failure or Ctrl-C.

## What we patch

| Driver             | Change                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| **better-sqlite3** | Retry on `SQLITE_BUSY`; fail fast on `SQLITE_BUSY_SNAPSHOT` inside a transaction.                                                     |
| **postgres**       | `onConnect` / `onRelease` pool hooks via `extendPostgresDriver()` — Beacon uses these for per-request RLS (`SET app.current_tenant`). |

Covered by:

-   `test/functional/driver/better-sqlite3/better-sqlite3-busy-error-retry.test.ts`
-   `test/functional/driver/postgres/postgres-driver-extension.test.ts`

## Don't run `pnpm test` directly

Upstream's sample `ormconfig.json` enables the plain `sqlite` driver with the `query` load strategy and concurrent connections.
Both have pre-existing upstream bugs that upstream CI never exercises.
A stock `pnpm test` reports ~7 failures unrelated to our patches.
`beacon/test.sh` scopes to the drivers we actually changed.
