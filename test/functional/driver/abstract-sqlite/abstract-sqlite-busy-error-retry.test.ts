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
    expectBothSqliteDrivers,
    lockDatabase,
    openSecondHandle,
    WRITE_QUERY,
} from "./sqlite-lease-test-utils"

const SQLITE_DRIVERS: ("sqlite" | "better-sqlite3")[] = [
    "sqlite",
    "better-sqlite3",
]

describe("sqlite driver > busy error retry", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [],
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: {
                timeout: 0,
                busyTimeout: 1,
                busyErrorRetryInterval: 25,
                busyErrorRetryTimeout: 30000,
            },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should retry a busy query until the other writer commits", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    const pendingWrite = connection.query(WRITE_QUERY)

                    // Long enough for several retries, and proof the wait is asynchronous:
                    // a blocking busy_timeout would never let this timer fire.
                    await new Promise((ok) => {
                        setTimeout(ok, 150)
                    })
                    expect(log.getRetryCount()).to.be.greaterThan(0)

                    await release()
                    await pendingWrite
                } finally {
                    log.restore()
                    await release()
                }
            }),
        )
    })

    it("should retry the BEGIN itself, so a contended transaction is never half started", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                const runner = connection.createQueryRunner()
                try {
                    // BEGIN IMMEDIATE runs at depth 0, so the retry rule covers it.
                    // The unit of work either starts whole or not at all,
                    // which is the point of taking the write lock up front.
                    const pendingBegin = runner.startTransaction()

                    await new Promise((ok) => {
                        setTimeout(ok, 150)
                    })
                    expect(log.getRetryCount()).to.be.greaterThan(0)

                    await release()
                    await pendingBegin
                    expect(runner.isTransactionActive).to.equal(true)

                    await runner.rollbackTransaction()
                } finally {
                    log.restore()
                    await release()
                    await runner.release()
                }
            }),
        )
    })

    it("should retry a COMMIT held up by a reader on another connection", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const reader = await openSecondHandle(connection)
                const log = captureLog(connection)
                const runner = connection.createQueryRunner()
                let isReaderOpen = false

                const closeReader = async () => {
                    if (isReaderOpen) {
                        isReaderOpen = false
                        await reader.exec("COMMIT")
                    }
                    await reader.close()
                }

                try {
                    await runner.startTransaction()
                    await runner.query(WRITE_QUERY)

                    // A read transaction holds a SHARED lock,
                    // and in rollback-journal mode that stops our COMMIT taking EXCLUSIVE.
                    // COMMIT has to be retried:
                    // a transaction left open would fail the next runner's BEGIN
                    // with SQLITE_ERROR, which is not a busy error and is never retried.
                    await reader.exec("BEGIN")
                    await reader.exec("SELECT count(*) FROM sqlite_master")
                    isReaderOpen = true

                    const pendingCommit = runner.commitTransaction()
                    await new Promise((ok) => {
                        setTimeout(ok, 150)
                    })

                    // The reader must actually have blocked the COMMIT,
                    // or this test passes without exercising the retry path.
                    expect(log.getRetryCount()).to.be.greaterThan(0)

                    await closeReader()
                    await pendingCommit

                    expect(runner.isTransactionActive).to.equal(false)
                } finally {
                    log.restore()
                    await closeReader()
                    await runner.release()
                }
            }),
        )
    })
})

describe("sqlite driver > busy error retry > no retry configured", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [],
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: { timeout: 0, busyTimeout: 1 },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should not retry when busyErrorRetryInterval is unset", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    await connection.query(WRITE_QUERY).should.be.rejected
                    expect(log.getRetryCount()).to.equal(0)
                } finally {
                    log.restore()
                    await release()
                }
            }),
        )
    })

    it("should free the connection when BEGIN fails", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const runner = connection.createQueryRunner()
                try {
                    // No retries configured, so BEGIN IMMEDIATE fails at once.
                    let hasFailed = false
                    try {
                        await runner.startTransaction()
                    } catch {
                        hasFailed = true
                    }
                    expect(hasFailed).to.equal(true)
                    expect(runner.isTransactionActive).to.equal(false)

                    await release()

                    // Completes promptly only if the failed BEGIN freed the lease
                    // instead of holding it to the lease timeout.
                    const startedAt = Date.now()
                    await connection.query(WRITE_QUERY)
                    expect(Date.now() - startedAt).to.be.lessThan(5000)
                } finally {
                    await runner.release()
                    await release()
                }
            }),
        )
    })
})

describe("sqlite driver > busy error retry > deadline reached", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [],
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: {
                timeout: 0,
                busyTimeout: 1,
                busyErrorRetryInterval: 10,
                busyErrorRetryTimeout: 150,
            },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should retry until busyErrorRetryTimeout elapses, then fail with the sqlite error", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    const startedAt = Date.now()
                    let failure: Error | undefined
                    try {
                        await connection.query(WRITE_QUERY)
                    } catch (err) {
                        failure = err as Error
                    }

                    // Rejects with the sqlite error itself, not a synthetic
                    // gave-up error, once the wall-clock budget is spent.
                    expect(failure).to.be.instanceOf(QueryFailedError)
                    expect(String(failure)).to.match(/database is locked/i)
                    expect(log.getRetryCount()).to.be.greaterThan(0)
                    expect(Date.now() - startedAt).to.be.greaterThanOrEqual(150)
                    expect(Date.now() - startedAt).to.be.lessThan(10000)
                } finally {
                    log.restore()
                    await release()
                }
            }),
        )
    })
})

describe("sqlite driver > busy error retry > default deadline", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [],
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: {
                timeout: 0,
                busyTimeout: 1,
                busyErrorRetryInterval: 25,
            },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should retry for the default 5000ms budget when busyErrorRetryTimeout is unset", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                const log = captureLog(connection)
                try {
                    const startedAt = Date.now()
                    await connection.query(WRITE_QUERY).should.be.rejected
                    expect(log.getRetryCount()).to.be.greaterThan(0)
                    expect(Date.now() - startedAt).to.be.greaterThanOrEqual(
                        4000,
                    )
                } finally {
                    log.restore()
                    await release()
                }
            }),
        )
    })
})

describe("sqlite driver > busy error retry > event loop", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [],
            enabledDrivers: SQLITE_DRIVERS,
            // A production-shaped config with a real busy timeout, not 0.
            // better-sqlite3 blocks inside C for the busy timeout on every attempt,
            // so that term decides whether retrying freezes the process.
            // A config that zeroes it would hide the freeze this test exists to catch.
            driverSpecific: {
                timeout: 200,
                busyTimeout: 200,
                busyErrorRetryInterval: 50,
                busyErrorRetryTimeout: 400,
            },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should keep the event loop running while it retries", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const release = await lockDatabase(connection)
                let tickCount = 0
                const ticker = setInterval(() => {
                    tickCount += 1
                }, 10)
                try {
                    await connection.query(WRITE_QUERY).should.be.rejected
                    // With the driver's default 5000ms busy timeout the tick count lands
                    // near zero, which is what made a nominal 200ms retry budget really
                    // cost 16 seconds.
                    expect(tickCount).to.be.greaterThan(3)
                } finally {
                    clearInterval(ticker)
                    await release()
                }
            }),
        )
    })
})
