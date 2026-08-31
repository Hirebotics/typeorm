import "reflect-metadata"
import { expect } from "chai"
import { DataSource, EntitySubscriberInterface } from "../../../../src"
import { QueryRunnerAlreadyReleasedError } from "../../../../src/error/QueryRunnerAlreadyReleasedError"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import { Thing } from "./entity/Thing"
import {
    captureLog,
    captureSql,
    expectBothSqliteDrivers,
} from "./sqlite-lease-test-utils"

const SQLITE_DRIVERS: ("sqlite" | "better-sqlite3")[] = [
    "sqlite",
    "better-sqlite3",
]

describe("sqlite driver > query runner ownership", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [__dirname + "/entity/*{.js,.ts}"],
            enabledDrivers: SQLITE_DRIVERS,
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should hand every caller its own query runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const first = connection.createQueryRunner()
                const second = connection.createQueryRunner()
                try {
                    expect(first).to.not.equal(second)
                    expect(first.manager).to.not.equal(second.manager)
                } finally {
                    await first.release()
                    await second.release()
                }
            }),
        )
    })

    it("should keep a committed transaction when a concurrent one rolls back", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const sql = captureSql(connection)
                try {
                    const results = await Promise.allSettled([
                        connection.transaction(async (manager) => {
                            await manager.save(Thing, { name: "rolled-back" })
                            await new Promise((ok) => {
                                setTimeout(ok, 30)
                            })
                            throw new Error("ordinary app failure")
                        }),
                        connection.transaction(async (manager) => {
                            await new Promise((ok) => {
                                setTimeout(ok, 10)
                            })
                            await manager.save(Thing, { name: "committed" })
                        }),
                    ])

                    expect(results[0].status).to.equal("rejected")
                    expect(results[1].status).to.equal("fulfilled")

                    const names = (
                        await connection.getRepository(Thing).find()
                    ).map((thing) => {
                        return thing.name
                    })

                    // Before the lease both units of work shared one runner,
                    // so the second one's insert became a savepoint inside the
                    // first one's transaction and died with its ROLLBACK,
                    // while its caller was told it had succeeded.
                    expect(names).to.eql(["committed"])

                    const control = sql.getTransactionControlStatements()
                    expect(
                        control.filter((s) => {
                            return /^\s*BEGIN IMMEDIATE/i.test(s)
                        }),
                    ).to.have.length(2)
                    expect(
                        control.filter((s) => {
                            return /SAVEPOINT typeorm_/i.test(s)
                        }),
                    ).to.have.length(0)
                } finally {
                    sql.restore()
                }
            }),
        )
    })

    it("should still use savepoints for a real nested transaction on one runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const sql = captureSql(connection)
                try {
                    await connection.transaction(async (manager) => {
                        const runner = manager.queryRunner!
                        await runner.startTransaction()
                        await manager.save(Thing, { name: "nested" })
                        await runner.commitTransaction()
                    })

                    const control = sql.getTransactionControlStatements()
                    expect(
                        control.filter((s) => {
                            return /^\s*SAVEPOINT typeorm_1/i.test(s)
                        }),
                    ).to.have.length(1)
                    expect(
                        control.filter((s) => {
                            return /^\s*RELEASE SAVEPOINT typeorm_1/i.test(s)
                        }),
                    ).to.have.length(1)
                } finally {
                    sql.restore()
                }
            }),
        )
    })

    it("should not let another runner read a transaction's uncommitted rows", () => {
        return Promise.all(
            connections.map(async (connection) => {
                let seenByOther: string[] = []

                const results = await Promise.allSettled([
                    connection.transaction(async (manager) => {
                        await manager.save(Thing, { name: "uncommitted" })
                        await new Promise((ok) => {
                            setTimeout(ok, 60)
                        })
                        throw new Error("ordinary app failure")
                    }),
                    (async () => {
                        await new Promise((ok) => {
                            setTimeout(ok, 20)
                        })
                        seenByOther = (
                            await connection.getRepository(Thing).find()
                        ).map((thing) => {
                            return thing.name
                        })
                    })(),
                ])

                // The reader must complete by waiting, not by failing or joining.
                expect(results[0].status).to.equal("rejected")
                expect(results[1].status).to.equal("fulfilled")

                // Sqlite has one connection,
                // so any statement issued while a transaction is open is inside that transaction.
                // The reader has to wait rather than join.
                expect(seenByOther).to.not.include("uncommitted")
            }),
        )
    })

    it("should roll back and free the connection when a runner is released mid-transaction", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const log = captureLog(connection)
                try {
                    const abandoned = connection.createQueryRunner()
                    await abandoned.startTransaction()
                    await abandoned.manager.save(Thing, { name: "abandoned" })
                    await abandoned.release()

                    expect(
                        log.getAbandonedTransactionRollbackCount(),
                    ).to.be.greaterThan(0)

                    // The connection is usable again, and the abandoned work is gone.
                    const names = (
                        await connection.getRepository(Thing).find()
                    ).map((thing) => {
                        return thing.name
                    })
                    expect(names).to.eql([])
                } finally {
                    log.restore()
                }
            }),
        )
    })

    it("should roll back and free the connection when a runner with a raw BEGIN is released", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A raw BEGIN opens a transaction in sqlite without setting any
                // runner flag, so teardown has to track it separately.
                const log = captureLog(connection)
                const runner = connection.createQueryRunner()
                try {
                    await runner.query("BEGIN TRANSACTION")
                    await runner.query(
                        `INSERT INTO thing (name) VALUES ('raw')`,
                    )
                } finally {
                    await runner.release()
                    log.restore()
                }

                expect(
                    log.getAbandonedTransactionRollbackCount(),
                ).to.be.greaterThan(0)

                const startedAt = Date.now()
                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => {
                    return thing.name
                })
                expect(names).to.eql([])
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        )
    })

    it("should reject queries on a released runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const runner = connection.createQueryRunner()
                await runner.query("SELECT 1")
                await runner.release()

                await runner
                    .query("SELECT 1")
                    .should.be.rejectedWith(QueryRunnerAlreadyReleasedError)

                // release() is idempotent.
                await runner.release()
            }),
        )
    })

    it("should roll back an orphaned outer transaction when a subscriber fails a nested begin", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // Upstream clears isTransactionActive when a BeforeTransactionStart
                // subscriber throws, even on a nested begin with the outer transaction
                // still open in sqlite. Teardown must still see and roll it back.
                const log = captureLog(connection)
                let shouldThrowOnNestedBegin = false
                const subscriber: EntitySubscriberInterface = {
                    beforeTransactionStart() {
                        if (shouldThrowOnNestedBegin) {
                            throw new Error("subscriber failure")
                        }
                    },
                }
                connection.subscribers.push(subscriber)
                const runner = connection.createQueryRunner()
                try {
                    await runner.startTransaction()
                    await runner.manager.save(Thing, { name: "orphaned" })

                    shouldThrowOnNestedBegin = true
                    let failure = "no error"
                    try {
                        await runner.startTransaction()
                    } catch (err) {
                        failure = (err as Error).message
                    }
                    expect(failure).to.equal("subscriber failure")
                } finally {
                    shouldThrowOnNestedBegin = false
                    await runner.release()
                    connection.subscribers.splice(
                        connection.subscribers.indexOf(subscriber),
                        1,
                    )
                    log.restore()
                }

                expect(
                    log.getAbandonedTransactionRollbackCount(),
                ).to.be.greaterThan(0)

                // The orphaned work is gone and the connection is free again.
                const startedAt = Date.now()
                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => {
                    return thing.name
                })
                expect(names).to.eql([])
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        )
    })

    it("should serialize an implicit save() transaction against an explicit one", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // EntityPersistExecutor opens its own transaction,
                // so a bare save() is a unit of work in its own right,
                // not a statement that can join a transaction another caller opened.
                const results = await Promise.allSettled([
                    connection.transaction(async (manager) => {
                        await manager.save(Thing, { name: "explicit" })
                        await new Promise((ok) => {
                            setTimeout(ok, 30)
                        })
                    }),
                    connection.getRepository(Thing).save({ name: "implicit" }),
                ])

                expect(
                    results.map((r) => {
                        return r.status
                    }),
                ).to.eql(["fulfilled", "fulfilled"])

                const names = (await connection.getRepository(Thing).find())
                    .map((thing) => {
                        return thing.name
                    })
                    .sort()
                expect(names).to.eql(["explicit", "implicit"])
            }),
        )
    })

    it("should not share the lease between data sources", async () => {
        // A regression to one module-global lease would serialize unrelated
        // databases and can deadlock an app coordinating two of them.
        expect(connections.length).to.be.greaterThan(1)
        const [first, second] = connections
        const runner = first.createQueryRunner()
        try {
            await runner.startTransaction()

            const startedAt = Date.now()
            await second.query("SELECT 1")
            expect(Date.now() - startedAt).to.be.lessThan(5000)
            expect(runner.isTransactionActive).to.equal(true)
        } finally {
            await runner.rollbackTransaction()
            await runner.release()
        }
    })
})

describe("sqlite driver > query runner ownership > lease timeout", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: [__dirname + "/entity/*{.js,.ts}"],
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: { connectionLeaseTimeout: 500 },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should fail with a diagnostic rather than hang when a runner waits on its own caller", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const startedAt = Date.now()
                let message = "no error"

                try {
                    await connection.transaction(async () => {
                        // Reaching past the transaction's own manager for a fresh runner
                        // makes the new runner wait on its own caller,
                        // and that caller can never release.
                        const independentRunner = connection.createQueryRunner()
                        try {
                            await independentRunner.query("SELECT 1")
                        } finally {
                            await independentRunner.release()
                        }
                    })
                } catch (err) {
                    message = (err as Error).message
                }

                expect(message).to.match(
                    /Timed out after \d+ms waiting for the sqlite connection/,
                )
                // Names both sides, so the offending call site is identifiable from the log.
                expect(message).to.contain("Waiting to run: SELECT 1")
                expect(message).to.contain("Blocked by:")
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        )
    })

    it("should share one acquisition across concurrent statements on one runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A regression to per-statement acquire would make the second
                // statement queue behind its own runner and time out.
                const runner = connection.createQueryRunner()
                try {
                    await Promise.all([
                        runner.query("SELECT 1"),
                        runner.query("SELECT 2"),
                    ])

                    await runner.startTransaction()
                    await Promise.all([
                        runner.query("SELECT 3"),
                        runner.query("SELECT 4"),
                    ])
                    await runner.commitTransaction()
                } finally {
                    await runner.release()
                }
            }),
        )
    })

    it("should retry acquisition on the next statement after a lease timeout", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const holder = connection.createQueryRunner()
                const waiter = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    await waiter
                        .query("SELECT 1")
                        .should.be.rejectedWith(/Timed out after \d+ms/)

                    await holder.commitTransaction()

                    // The failed acquisition must not be cached on the runner.
                    await waiter.query("SELECT 1")
                } finally {
                    await waiter.release()
                    await holder.release()
                }
            }),
        )
    })
})
