import { expect } from "chai"
import { DataSource, QueryRunner } from "../../../../src"

/**
 * Shared helpers for the sqlite connection-lease tests.
 *
 * Not a *.test.ts file on purpose:
 * beacon/test.sh names the compiled test files it runs, and this one has no tests of its own.
 */

/** The drivers every suite in this directory runs against. */
export const SQLITE_DRIVERS: ("sqlite" | "better-sqlite3")[] = [
    "sqlite",
    "better-sqlite3",
]

/** The entity files every suite in this directory loads. */
export const TEST_ENTITIES = [__dirname + "/entity/*{.js,.ts}"]

/**
 * `PRAGMA user_version = N` writes the database header,
 * so it needs the same write lock an INSERT does.
 * That makes it the cheapest way to provoke SQLITE_BUSY.
 */
export const WRITE_QUERY = "PRAGMA user_version = 7"

/**
 * Fails the suite when the beacon ormconfig swap or the enabledDrivers filter breaks,
 * so the driver tests cannot silently pass with an empty connections array.
 */
export function expectBothSqliteDrivers(connections: DataSource[]): void {
    const types = connections.map((connection) => {
        return connection.options.type
    })
    expect(types).to.include("sqlite")
    expect(types).to.include("better-sqlite3")
}

/**
 * A second handle on the same file, standing in for another process.
 *
 * It has to be opened with the *same* sqlite library as the driver under test.
 * Two different builds of sqlite in one process cannot block each other at all:
 * file locks are POSIX advisory locks, which never conflict within a process,
 * and sqlite's own in-process lock table is per-library.
 * A better-sqlite3 handle would write straight through a node-sqlite3 transaction,
 * so a test written that way silently measures nothing.
 *
 * The busy timeout is 0, so this handle never waits on a lock either.
 */
export interface SqliteSecondHandle {
    exec(sql: string): Promise<void>
    close(): Promise<void>
}

/** The better-sqlite3 module is itself the Database constructor. */
type BetterSqlite3Module = new (path: string, options: { timeout: number }) => {
    exec(sql: string): void
    close(): void
}

interface NodeSqliteDatabase {
    exec(sql: string, callback: (err: Error | null) => void): void
    close(callback: () => void): void
}

interface NodeSqliteModule {
    Database: new (
        path: string,
        callback: (err: Error | null) => void,
    ) => NodeSqliteDatabase
}

export async function openSecondHandle(
    connection: DataSource,
): Promise<SqliteSecondHandle> {
    const sqliteModule = (connection.driver as unknown as { sqlite: unknown })
        .sqlite
    const database = (connection.options as { database: string }).database

    if (connection.options.type === "better-sqlite3") {
        const BetterSqlite3 = sqliteModule as BetterSqlite3Module
        const handle = new BetterSqlite3(database, { timeout: 0 })
        return {
            exec: async (sql: string) => {
                handle.exec(sql)
            },
            close: async () => {
                handle.close()
            },
        }
    }

    // node-sqlite3: async constructor, callback API throughout.
    const { Database } = sqliteModule as NodeSqliteModule
    const handle = await new Promise<NodeSqliteDatabase>((ok, fail) => {
        const db = new Database(database, (err) => {
            if (err) {
                fail(err)
            } else {
                ok(db)
            }
        })
    })
    const exec = (sql: string) => {
        return new Promise<void>((ok, fail) => {
            handle.exec(sql, (err) => {
                if (err) {
                    fail(err)
                } else {
                    ok()
                }
            })
        })
    }

    await exec("PRAGMA busy_timeout = 0")

    return {
        exec,
        close: () => {
            return new Promise<void>((ok) => {
                handle.close(() => {
                    ok()
                })
            })
        },
    }
}

/**
 * Holds a write lock on the connection's database file until the returned function is awaited.
 */
export async function lockDatabase(
    connection: DataSource,
): Promise<() => Promise<void>> {
    const handle = await openSecondHandle(connection)
    await handle.exec("BEGIN IMMEDIATE")

    let hasReleased = false
    return async () => {
        if (hasReleased) {
            return
        }
        hasReleased = true
        await handle.exec("COMMIT")
        await handle.close()
    }
}

/**
 * Collects what the query runner logs,
 * so retries and lease behaviour can be asserted without depending on wall-clock timing.
 */
export function captureLog(connection: DataSource) {
    const messages: string[] = []
    const logger = connection.logger
    const original = logger.log

    logger.log = (
        level: "log" | "info" | "warn",
        message: unknown,
        queryRunner?: QueryRunner,
    ) => {
        if (typeof message === "string") {
            messages.push(`${level}: ${message}`)
        }
        return original.call(logger, level, message, queryRunner)
    }

    const countMessagesMatching = (pattern: RegExp) => {
        return messages.filter((message) => {
            return pattern.test(message)
        }).length
    }

    return {
        getRetryCount: () => {
            return countMessagesMatching(/SQLITE_BUSY, retrying/)
        },
        getAbandonedTransactionRollbackCount: () => {
            return countMessagesMatching(
                /released with a transaction still open/,
            )
        },
        restore: () => {
            logger.log = original
        },
    }
}

/**
 * Collects the SQL the connection logs,
 * so the statements a unit of work really issued can be asserted.
 */
export function captureSql(connection: DataSource) {
    const statements: string[] = []
    const logger = connection.logger
    const original = logger.logQuery

    logger.logQuery = (
        query: string,
        parameters?: unknown[],
        queryRunner?: QueryRunner,
    ) => {
        statements.push(query)
        return original.call(logger, query, parameters, queryRunner)
    }

    return {
        getTransactionControlStatements: () => {
            return statements.filter((sql) => {
                return /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.test(
                    sql,
                )
            })
        },
        restore: () => {
            logger.logQuery = original
        },
    }
}
