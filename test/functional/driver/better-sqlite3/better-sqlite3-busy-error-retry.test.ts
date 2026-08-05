import "reflect-metadata"
import { expect } from "chai"
import { DataSource } from "../../../../src"
import { QueryFailedError } from "../../../../src/error/QueryFailedError"
import { BetterSqlite3ConnectionOptions } from "../../../../src/driver/better-sqlite3/BetterSqlite3ConnectionOptions"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"

/**
 * `PRAGMA user_version = N` writes the database header, so it needs the same write lock
 * an INSERT does -- which makes it the cheapest way to provoke SQLITE_BUSY.
 */
const WRITE_QUERY = "PRAGMA user_version = 7"

/**
 * Holds a write lock on the connection's database file until released.
 * `timeout: 0` so this handle never waits on the lock either.
 */
function lockDatabase(connection: DataSource) {
    const options = connection.options as BetterSqlite3ConnectionOptions
    const sqlite = connection.driver as unknown as { sqlite: any }
    const handle = new sqlite.sqlite(options.database, { timeout: 0 })
    handle.exec("BEGIN IMMEDIATE")

    let released = false
    return () => {
        if (released) return
        released = true
        handle.exec("COMMIT")
        handle.close()
    }
}

/**
 * Collects the retry messages the query runner logs, so retry counts can be asserted
 * without depending on wall-clock timing.
 */
function captureRetryLog(connection: DataSource) {
    const messages: string[] = []
    const logger = connection.logger as any
    const original = logger.log

    logger.log = (level: string, message: any, queryRunner?: any) => {
        if (typeof message === "string" && message.startsWith("Sqlite is busy"))
            messages.push(`${level}: ${message}`)

        return original.call(logger, level, message, queryRunner)
    }

    return {
        retries: () => messages.filter((m) => m.startsWith("info:")).length,
        givenUp: () => messages.filter((m) => m.startsWith("warn:")).length,
        restore: () => {
            logger.log = original
        },
    }
}

describe("better-sqlite3 driver > busy error retry > until the lock clears", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: ["better-sqlite3"],
                driverSpecific: {
                    timeout: 0,
                    busyErrorRetryInterval: 25,
                    // 0 means retry forever
                    busyErrorRetryLimit: 0,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry a SQLITE_BUSY query until the other writer commits", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = lockDatabase(connection)
                const log = captureRetryLog(connection)
                try {
                    const pending = connection.query(WRITE_QUERY)

                    // Long enough for several retries, and proves the wait is async:
                    // a blocking busy_timeout would never let this timer fire.
                    await new Promise((ok) => setTimeout(ok, 150))
                    expect(log.retries()).to.be.greaterThan(0)

                    release()
                    await pending

                    expect(log.givenUp()).to.equal(0)
                } finally {
                    log.restore()
                    release()
                }
            }),
        ))
})

describe("better-sqlite3 driver > busy error retry > limit reached", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: ["better-sqlite3"],
                driverSpecific: {
                    timeout: 0,
                    busyErrorRetryInterval: 5,
                    busyErrorRetryLimit: 2,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry exactly busyErrorRetryLimit times, then fail with the sqlite error", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = lockDatabase(connection)
                const log = captureRetryLog(connection)
                try {
                    const error = await connection.query(WRITE_QUERY).then(
                        () => undefined,
                        (err) => err,
                    )

                    expect(error).to.be.instanceOf(QueryFailedError)
                    expect(error.driverError.code).to.match(/^SQLITE_BUSY/)
                    expect(log.retries()).to.equal(2)
                    expect(log.givenUp()).to.equal(1)
                } finally {
                    log.restore()
                    release()
                }
            }),
        ))
})

describe("better-sqlite3 driver > busy error retry > disabled by default", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: ["better-sqlite3"],
                driverSpecific: {
                    timeout: 0,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should not retry when busyErrorRetryInterval is unset", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = lockDatabase(connection)
                const log = captureRetryLog(connection)
                try {
                    const error = await connection.query(WRITE_QUERY).then(
                        () => undefined,
                        (err) => err,
                    )

                    expect(error).to.be.instanceOf(QueryFailedError)
                    expect(error.driverError.code).to.match(/^SQLITE_BUSY/)
                    expect(log.retries()).to.equal(0)
                } finally {
                    log.restore()
                    release()
                }
            }),
        ))
})
