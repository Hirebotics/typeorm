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
 * A different value, so the other connection's write really changes the database.
 * A commit that changes nothing leaves an open snapshot valid.
 */
const OTHER_WRITE_QUERY = "PRAGMA user_version = 11"

/**
 * A second handle on the same file, standing in for another process.
 * `timeout: 0` so this handle never waits on a lock either.
 */
function openDatabaseHandle(connection: DataSource) {
    const options = connection.options as BetterSqlite3ConnectionOptions
    const sqlite = connection.driver as unknown as { sqlite: any }
    return new sqlite.sqlite(options.database, { timeout: 0 })
}

/**
 * Holds a write lock on the connection's database file until released.
 *
 * `writeBeforeRelease` commits an actual change on the way out, which is what
 * invalidates the snapshot of a read transaction opened before it.
 */
function lockDatabase(connection: DataSource, writeBeforeRelease = false) {
    const handle = openDatabaseHandle(connection)
    handle.exec("BEGIN IMMEDIATE")

    let released = false
    return () => {
        if (released) return
        released = true
        if (writeBeforeRelease) handle.exec(OTHER_WRITE_QUERY)
        handle.exec("COMMIT")
        handle.close()
    }
}

/**
 * Commits a write from another connection and leaves no lock behind,
 * so the only thing that can still fail afterwards is a stale snapshot.
 */
function writeFromAnotherConnection(connection: DataSource) {
    const handle = openDatabaseHandle(connection)
    handle.exec(OTHER_WRITE_QUERY)
    handle.close()
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
        if (typeof message === "string") messages.push(`${level}: ${message}`)

        return original.call(logger, level, message, queryRunner)
    }

    const count = (pattern: RegExp) =>
        messages.filter((message) => pattern.test(message)).length

    return {
        retries: () => count(/^info: Sqlite is busy, retrying/),
        givenUp: () => count(/^warn: Sqlite is busy and the retry limit/),
        failedFast: () => count(/^warn: SQLITE_BUSY_SNAPSHOT/),
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

describe("better-sqlite3 driver > busy error retry > snapshot inside a transaction", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: ["better-sqlite3"],
                driverSpecific: {
                    timeout: 0,
                    // SQLITE_BUSY_SNAPSHOT only exists in WAL mode.
                    enableWAL: true,
                    // The settings cloud-connector runs with: this case used to stall 10 seconds.
                    busyErrorRetryInterval: 1000,
                    busyErrorRetryLimit: 10,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(async () => {
        // journal_mode is stored in the database file itself,
        // so hand the file back in the mode the other tests expect.
        await Promise.all(
            connections.map((connection) =>
                connection.query("PRAGMA journal_mode = DELETE"),
            ),
        )
        await closeTestingConnections(connections)
    })

    it("should fail immediately, without retrying, and say why", () =>
        Promise.all(
            connections.map(async (connection) => {
                const queryRunner = connection.createQueryRunner()
                const log = captureRetryLog(connection)
                try {
                    await queryRunner.startTransaction()
                    // Reading is what pins the snapshot -- BEGIN alone pins nothing.
                    await queryRunner.query("SELECT * FROM sqlite_master")

                    // No lock is left behind, so the stale snapshot is the only thing left to fail on.
                    writeFromAnotherConnection(connection)

                    const startedAt = Date.now()
                    const error = await queryRunner.query(WRITE_QUERY).then(
                        () => undefined,
                        (err) => err,
                    )

                    expect(error).to.be.instanceOf(QueryFailedError)
                    expect(error.driverError.code).to.equal(
                        "SQLITE_BUSY_SNAPSHOT",
                    )
                    expect(log.retries()).to.equal(0)
                    expect(log.givenUp()).to.equal(0)
                    expect(log.failedFast()).to.equal(1)
                    // A single retry would already have cost 1000ms.
                    expect(Date.now() - startedAt).to.be.lessThan(500)

                    // The remedy the warning names has to be a real one.
                    await queryRunner.rollbackTransaction()
                    await queryRunner.query(WRITE_QUERY)
                } finally {
                    log.restore()
                    if (queryRunner.isTransactionActive)
                        await queryRunner.rollbackTransaction()
                    await queryRunner.release()
                }
            }),
        ))

    it("should retry a plain SQLITE_BUSY and stop once it turns into SQLITE_BUSY_SNAPSHOT", () =>
        Promise.all(
            connections.map(async (connection) => {
                const queryRunner = connection.createQueryRunner()
                const release = lockDatabase(connection, true)
                const log = captureRetryLog(connection)
                try {
                    await queryRunner.startTransaction()
                    await queryRunner.query("SELECT * FROM sqlite_master")

                    const pending = queryRunner.query(WRITE_QUERY).then(
                        () => undefined,
                        (err) => err,
                    )

                    // The other writer still holds the lock, so this attempt is a plain
                    // SQLITE_BUSY and is retried exactly as it was before.
                    await new Promise((ok) => setTimeout(ok, 100))
                    expect(log.retries()).to.equal(1)

                    // Its commit is what makes this transaction's snapshot stale.
                    release()

                    const error = await pending

                    expect(error).to.be.instanceOf(QueryFailedError)
                    expect(error.driverError.code).to.equal(
                        "SQLITE_BUSY_SNAPSHOT",
                    )
                    expect(log.retries()).to.equal(1)
                    expect(log.givenUp()).to.equal(0)
                    expect(log.failedFast()).to.equal(1)
                } finally {
                    log.restore()
                    release()
                    if (queryRunner.isTransactionActive)
                        await queryRunner.rollbackTransaction()
                    await queryRunner.release()
                }
            }),
        ))
})

describe("better-sqlite3 driver > busy error retry > snapshot outside a transaction", () => {
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

    it("should still retry, because each statement takes a fresh snapshot", () =>
        Promise.all(
            connections.map(async (connection) => {
                const queryRunner = connection.createQueryRunner() as any
                const log = captureRetryLog(connection)
                try {
                    // The error is injected: outside a transaction sqlite takes a fresh snapshot
                    // per statement, so the only way to hold a stale one is an open iterator,
                    // which this driver never opens. Retrying still has to be the behaviour.
                    queryRunner.getStmt = () =>
                        Promise.reject(
                            Object.assign(new Error("database is locked"), {
                                code: "SQLITE_BUSY_SNAPSHOT",
                            }),
                        )

                    const error = await queryRunner.query(WRITE_QUERY).then(
                        () => undefined,
                        (err: any) => err,
                    )

                    expect(error).to.be.instanceOf(QueryFailedError)
                    expect(error.driverError.code).to.equal(
                        "SQLITE_BUSY_SNAPSHOT",
                    )
                    expect(log.retries()).to.equal(2)
                    expect(log.givenUp()).to.equal(1)
                    expect(log.failedFast()).to.equal(0)
                } finally {
                    log.restore()
                    await queryRunner.release()
                }
            }),
        ))
})
