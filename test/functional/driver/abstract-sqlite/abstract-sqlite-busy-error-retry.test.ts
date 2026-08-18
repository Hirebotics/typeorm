import "reflect-metadata"
import { expect } from "chai"
import { DataSource } from "../../../../src"
import { QueryFailedError } from "../../../../src/error/QueryFailedError"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import {
    captureLog,
    lockDatabase,
    openSecondHandle,
    WRITE_QUERY,
} from "./sqlite-lease-test-utils"

const SQLITE_DRIVERS: ("sqlite" | "better-sqlite3")[] = [
    "sqlite",
    "better-sqlite3",
]

describe("sqlite driver > busy error retry > until the lock clears", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: {
                    timeout: 0,
                    busyTimeout: 1,
                    busyErrorRetryInterval: 25,
                    busyErrorRetryLimit: 100,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry a busy query until the other writer commits", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    const pending = connection.query(WRITE_QUERY)

                    // Long enough for several retries, and proof the wait is asynchronous:
                    // a blocking busy_timeout would never let this timer fire.
                    await new Promise((ok) => setTimeout(ok, 150))
                    expect(log.retries()).to.be.greaterThan(0)

                    await release()
                    await pending
                } finally {
                    log.restore()
                    await release()
                }
            }),
        ))
})

describe("sqlite driver > busy error retry > limit reached", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: {
                    timeout: 0,
                    busyTimeout: 1,
                    busyErrorRetryInterval: 10,
                    busyErrorRetryLimit: 3,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry exactly busyErrorRetryLimit times, then fail with the sqlite error", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    let failure: Error | undefined
                    try {
                        await connection.query(WRITE_QUERY)
                    } catch (err) {
                        failure = err as Error
                    }

                    expect(failure).to.be.instanceOf(QueryFailedError)
                    expect(String(failure)).to.match(/database is locked/i)
                    // There is no infinite mode: the budget is always bounded.
                    expect(log.retries()).to.equal(3)
                } finally {
                    log.restore()
                    await release()
                }
            }),
        ))
})

describe("sqlite driver > busy error retry > disabled by default", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: { timeout: 0, busyTimeout: 1 },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should not retry when busyErrorRetryInterval is unset", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    await connection.query(WRITE_QUERY).should.be.rejected
                    expect(log.retries()).to.equal(0)
                } finally {
                    log.restore()
                    await release()
                }
            }),
        ))
})

describe("sqlite driver > busy error retry > transaction boundaries", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: {
                    timeout: 0,
                    busyTimeout: 1,
                    busyErrorRetryInterval: 25,
                    busyErrorRetryLimit: 100,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry the BEGIN itself, so a contended transaction is never half started", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                const runner = connection.createQueryRunner()
                try {
                    // BEGIN IMMEDIATE runs at depth 0, so the retry rule covers it. This is
                    // the whole point of taking the write lock up front: the unit of work
                    // either starts or it does not, never half.
                    const pending = runner.startTransaction()

                    await new Promise((ok) => setTimeout(ok, 150))
                    expect(log.retries()).to.be.greaterThan(0)

                    await release()
                    await pending
                    expect(runner.isTransactionActive).to.equal(true)

                    await runner.rollbackTransaction()
                } finally {
                    log.restore()
                    await release()
                    await runner.release()
                }
            }),
        ))
})

describe("sqlite driver > busy error retry > event loop", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                // A production-shaped config: a real busy timeout, not the 0 the older tests
                // used. better-sqlite3 blocks inside C for this on every attempt, so it is
                // the term that decides whether retrying freezes the process.
                driverSpecific: {
                    timeout: 200,
                    busyTimeout: 200,
                    busyErrorRetryInterval: 50,
                    busyErrorRetryLimit: 3,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should keep the event loop running while it retries", () =>
        Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                let ticks = 0
                const ticker = setInterval(() => ticks++, 10)
                try {
                    await connection.query(WRITE_QUERY).should.be.rejected
                    // With the driver's default 5000ms busy timeout this is near zero, which
                    // is what made a "200ms" retry budget really cost 16 seconds.
                    expect(ticks).to.be.greaterThan(3)
                } finally {
                    clearInterval(ticker)
                    await release()
                }
            }),
        ))
})

describe("sqlite driver > busy error retry > commit", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: {
                    timeout: 0,
                    busyTimeout: 1,
                    busyErrorRetryInterval: 25,
                    busyErrorRetryLimit: 100,
                },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should retry a COMMIT held up by a reader on another connection", () =>
        Promise.all(
            connections.map(async (connection) => {
                const reader = await openSecondHandle(connection)
                const log = captureLog(connection)
                const runner = connection.createQueryRunner()
                let readerOpen = false

                const closeReader = async () => {
                    if (readerOpen) {
                        readerOpen = false
                        await reader.exec("COMMIT")
                    }
                    await reader.close()
                }

                try {
                    await runner.startTransaction()
                    await runner.query(WRITE_QUERY)

                    // A read transaction holds a SHARED lock, which in rollback-journal mode
                    // stops our COMMIT taking EXCLUSIVE. COMMIT has to be retried: leaving the
                    // transaction open would make the next runner's BEGIN fail with
                    // SQLITE_ERROR, which is not busy and so is never retried.
                    await reader.exec("BEGIN")
                    await reader.exec("SELECT count(*) FROM sqlite_master")
                    readerOpen = true

                    const pending = runner.commitTransaction()
                    await new Promise((ok) => setTimeout(ok, 150))

                    await closeReader()
                    await pending

                    expect(runner.isTransactionActive).to.equal(false)
                } finally {
                    log.restore()
                    await closeReader()
                    await runner.release()
                }
            }),
        ))
})
