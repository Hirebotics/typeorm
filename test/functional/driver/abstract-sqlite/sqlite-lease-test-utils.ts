import { DataSource } from "../../../../src"

/**
 * Shared helpers for the sqlite connection-lease tests.
 *
 * Not a *.test.ts file on purpose: beacon/test.sh names the compiled test files it runs, and
 * this one has no tests of its own.
 */

/**
 * `PRAGMA user_version = N` writes the database header, so it needs the same write lock an
 * INSERT does, which makes it the cheapest way to provoke SQLITE_BUSY.
 */
export const WRITE_QUERY = "PRAGMA user_version = 7"

/**
 * A different value, so the other connection's write really changes the database.
 * A commit that changes nothing leaves an open snapshot valid.
 */
export const OTHER_WRITE_QUERY = "PRAGMA user_version = 11"

/**
 * A second handle on the same file, standing in for another process.
 *
 * It has to be opened with the *same* sqlite library as the driver under test. Two different
 * builds of sqlite in one process cannot block each other at all: file locks are POSIX advisory
 * locks, which never conflict within a process, and sqlite's own in-process lock table is
 * per-library. A better-sqlite3 handle would write straight through a node-sqlite3 transaction,
 * so a test written that way silently measures nothing.
 *
 * The busy timeout is 0 so this handle never waits on a lock either.
 */
export interface SecondHandle {
    exec(sql: string): Promise<void>
    close(): Promise<void>
}

export async function openSecondHandle(
    connection: DataSource,
): Promise<SecondHandle> {
    const sqlite = (connection.driver as unknown as { sqlite: any }).sqlite
    const database = (connection.options as { database?: string }).database

    if (connection.options.type === "better-sqlite3") {
        const handle = new sqlite(database, { timeout: 0 })
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
    const handle = await new Promise<any>((ok, fail) => {
        const db = new sqlite.Database(database, (err: any) =>
            err ? fail(err) : ok(db),
        )
    })
    const exec = (sql: string) =>
        new Promise<void>((ok, fail) =>
            handle.exec(sql, (err: any) => (err ? fail(err) : ok())),
        )

    await exec("PRAGMA busy_timeout = 0")

    return {
        exec,
        close: () => new Promise<void>((ok) => handle.close(() => ok())),
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

    let released = false
    return async () => {
        if (released) return
        released = true
        await handle.exec("COMMIT")
        await handle.close()
    }
}

/**
 * Collects what the query runner logs, so retries and lease behaviour can be asserted
 * without depending on wall-clock timing.
 */
export function captureLog(connection: DataSource) {
    const messages: string[] = []
    const logger = connection.logger as any
    const original = logger.log

    logger.log = (level: string, message: any, queryRunner?: any) => {
        if (typeof message === "string") messages.push(`${level}: ${message}`)
        return original.call(logger, level, message, queryRunner)
    }

    const count = (pattern: RegExp) =>
        messages.filter((message) => pattern.test(message)).length

    return {
        all: () => messages.slice(),
        retries: () => count(/SQLITE_BUSY, retrying/),
        leaseWaits: () => count(/Waited \d+ms for the sqlite connection/),
        leakRecoveries: () => count(/released with a transaction still open/),
        restore: () => {
            logger.log = original
        },
    }
}

/**
 * Collects the SQL the connection logs, so the statements a unit of work really issued
 * can be asserted.
 */
export function captureSql(connection: DataSource) {
    const statements: string[] = []
    const logger = connection.logger as any
    const original = logger.logQuery

    logger.logQuery = (
        query: string,
        parameters?: any[],
        queryRunner?: any,
    ) => {
        statements.push(query)
        return original.call(logger, query, parameters, queryRunner)
    }

    return {
        all: () => statements.slice(),
        /** Just the transaction-control statements, which is what ownership turns on. */
        transactionControl: () =>
            statements.filter((sql) =>
                /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.test(sql),
            ),
        clear: () => {
            statements.length = 0
        },
        restore: () => {
            logger.logQuery = original
        },
    }
}
