import "reflect-metadata"
import { expect } from "chai"
import {
    DataSource,
    EntitySubscriberInterface,
    QueryRunner,
} from "../../../../src"
import { TypeORMError } from "../../../../src/error/TypeORMError"
import { QueryRunnerAlreadyReleasedError } from "../../../../src/error/QueryRunnerAlreadyReleasedError"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import { Thing } from "./entity/Thing"
import {
    captureExecutedSql,
    captureLog,
    captureSql,
    executeOutOfBand,
    expectBothSqliteDrivers,
    SQLITE_DRIVERS,
    TEST_ENTITIES,
} from "./sqlite-lease-test-utils"

describe("sqlite driver > query runner ownership", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: TEST_ENTITIES,
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

                    // Before the lease both units of work shared one runner.
                    // The second insert became a savepoint inside the first transaction,
                    // it died with that transaction's ROLLBACK,
                    // and its caller was told it had succeeded.
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
                // A raw BEGIN opens a transaction in sqlite without setting any runner flag.
                // Teardown has to track it separately.
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

    it("should free the connection when the release rollback fails", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // Roll the transaction back behind the serializer's back.
                // Sqlite then has no transaction while the serializer still
                // believes one is open, so the ROLLBACK that release() issues
                // fails for real.
                // Without the recovery the connection would stay held for the
                // life of the driver and every later runner would time out.
                const abandoned = connection.createQueryRunner()
                await abandoned.startTransaction()
                await abandoned.manager.save(Thing, { name: "abandoned" })
                await executeOutOfBand(connection, "ROLLBACK")

                // release() must swallow the failure rather than throw.
                await abandoned.release()

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

    it("should roll back an orphaned outer transaction when a subscriber fails a nested begin", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // Upstream clears isTransactionActive when a BeforeTransactionStart
                // subscriber throws.
                // On a nested begin the outer transaction is still open in sqlite.
                // Teardown must still see it and roll it back.
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

    it("should refuse a statement that races release(), rather than committing it in autocommit", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const runner = connection.createQueryRunner()
                let racingOutcome = "never ran"
                try {
                    await runner.startTransaction()
                    await runner.query(
                        `INSERT INTO thing (name) VALUES ('inside-transaction')`,
                    )

                    // release() rolls back the abandoned transaction. A statement
                    // arriving during that rollback must not slip through and commit
                    // on its own.
                    const releasing = runner.release()
                    const racing = runner
                        .query(
                            `INSERT INTO thing (name) VALUES ('after-release')`,
                        )
                        .then(
                            () => "resolved",
                            (err: Error) => err.constructor.name,
                        )
                    ;[, racingOutcome] = await Promise.all([releasing, racing])
                } finally {
                    await runner.release()
                }

                expect(racingOutcome).to.equal(
                    QueryRunnerAlreadyReleasedError.name,
                )
                // Neither row survives: the first was rolled back, the second refused.
                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => thing.name)
                expect(names).to.eql([])
            }),
        )
    })

    it("should hand the connection back exactly once when release() is called twice concurrently", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const holder = connection.createQueryRunner()
                const first = connection.createQueryRunner()
                const second = connection.createQueryRunner()
                try {
                    await holder.startTransaction()

                    // Both queue behind the holder before it is released, so a double
                    // release grants the connection to two runners at the same time -
                    // the exact collision this patch exists to prevent.
                    let hasFirstStarted = false
                    let hasSecondStarted = false
                    const firstBegin = first.startTransaction().then(() => {
                        hasFirstStarted = true
                    })
                    const secondBegin = second.startTransaction().then(() => {
                        hasSecondStarted = true
                    })

                    await holder.rollbackTransaction()
                    await Promise.all([holder.release(), holder.release()])

                    await firstBegin
                    expect(hasFirstStarted).to.equal(true)
                    expect(hasSecondStarted).to.equal(false)

                    await first.commitTransaction()
                    await first.release()
                    await secondBegin
                    expect(hasSecondStarted).to.equal(true)
                    await second.commitTransaction()
                } finally {
                    await second.release()
                    await first.release()
                    await holder.release()
                }
            }),
        )
    })

    it("should release an unused runner promptly while another holds a transaction", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const holder = connection.createQueryRunner()
                const unused = connection.createQueryRunner()
                try {
                    await holder.startTransaction()

                    // This runner never ran a statement, so it holds nothing and must
                    // not wait on the connection just to be released.
                    const startedAt = Date.now()
                    await unused.release()
                    expect(Date.now() - startedAt).to.be.lessThan(1000)
                } finally {
                    await holder.rollbackTransaction()
                    await holder.release()
                }
            }),
        )
    })

    it("should not roll back the holder's transaction when a queued runner is released", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A runner that is still waiting in line never touched the
                // connection. Releasing it must leave the holder alone and must
                // not consume the turn it was waiting for, or the connection is
                // granted later with nobody left to give it back.
                const holder = connection.createQueryRunner()
                const queued = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    await holder.query(
                        `INSERT INTO thing (name) VALUES ('holder')`,
                    )

                    // .then, not await: this statement has to stay in flight
                    // across the release below.
                    const waiting = queued.query("SELECT 1").then(
                        () => "resolved",
                        (err: Error) => err.constructor.name,
                    )
                    await queued.release()

                    const held = await holder.query(`SELECT name FROM thing`)
                    expect(held).to.eql([{ name: "holder" }])
                    expect(await waiting).to.equal(
                        QueryRunnerAlreadyReleasedError.name,
                    )
                    await holder.commitTransaction()
                } finally {
                    await queued.release()
                    await holder.release()
                }

                // The turn the queued runner gave up must not have been spent.
                const startedAt = Date.now()
                await connection.query("SELECT 1")
                expect(Date.now() - startedAt).to.be.lessThan(1000)

                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => thing.name)
                expect(names).to.eql(["holder"])
            }),
        )
    })

    it("should not take a fresh connection for a runner that was already released", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // connect() is the only route to the connection, so it is the
                // one place that has to refuse a released runner. Handing one
                // out would lease it to a runner nobody will release, and wedge
                // the driver for the life of the process.
                const connectOutcome = async (runner: QueryRunner) => {
                    try {
                        await runner.connect()
                        return "resolved"
                    } catch (err) {
                        return (err as Error).constructor.name
                    }
                }

                // A runner that used the connection keeps its revoked lease,
                // and the lease refuses.
                const used = connection.createQueryRunner()
                await used.query("SELECT 1")
                await used.release()
                expect(await connectOutcome(used)).to.equal(
                    QueryRunnerAlreadyReleasedError.name,
                )

                // A runner released without ever connecting has no lease to
                // refuse for it, so only the released check stands between it
                // and a connection it would never give back.
                const unused = connection.createQueryRunner()
                await unused.release()
                expect(await connectOutcome(unused)).to.equal(
                    QueryRunnerAlreadyReleasedError.name,
                )

                // The same runner, but without awaiting the release first.
                // release() yields before it finishes even with no lease to
                // give back, and a first statement in that window used to be
                // granted the connection and keep it for good.
                const racing = connection.createQueryRunner()
                const releasing = racing.release()
                const raced = connectOutcome(racing)
                await releasing
                expect(await raced).to.equal(
                    QueryRunnerAlreadyReleasedError.name,
                )

                const startedAt = Date.now()
                await connection.query("SELECT 1")
                expect(Date.now() - startedAt).to.be.lessThan(1000)
            }),
        )
    })

    it("should reject a queued runner when the DataSource is destroyed", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const holder = connection.createQueryRunner()
                await holder.startTransaction()

                // Queues behind the transaction and never gets the connection.
                const queued = connection
                    .createQueryRunner()
                    .query("SELECT 1")
                    .then(
                        () => "resolved",
                        (err: Error) => err.message,
                    )

                const startedAt = Date.now()
                await connection.destroy()
                const outcome = await queued

                // Without teardown the waiter either burns the full acquire deadline
                // or is granted a handle that is already closed.
                expect(outcome).to.contain("DataSource was destroyed")
                expect(Date.now() - startedAt).to.be.lessThan(5000)

                await connection.initialize()
            }),
        )
    })

    it("should not interleave two runners' statements on the connection", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // The whole point of leasing, asserted on the statements that
                // actually reached sqlite rather than on the rows they left.
                // Isolation tests pass even if statements interleave, as long
                // as the outcome happens to survive.
                const runUnit = async (tag: string) => {
                    const runner = connection.createQueryRunner()
                    try {
                        await runner.startTransaction()
                        await runner.query(
                            `INSERT INTO thing (name) VALUES ('${tag}-a')`,
                        )
                        await runner.query(
                            `INSERT INTO thing (name) VALUES ('${tag}-b')`,
                        )
                        await runner.commitTransaction()
                    } finally {
                        await runner.release()
                    }
                }

                const sql = captureExecutedSql(connection)
                try {
                    await Promise.all([
                        runUnit("u1"),
                        runUnit("u2"),
                        runUnit("u3"),
                    ])
                } finally {
                    sql.restore()
                }

                const relevant = sql.getStatements().filter((statement) => {
                    return /^(BEGIN IMMEDIATE|COMMIT|INSERT INTO thing)/.test(
                        statement,
                    )
                })
                expect(relevant.length).to.equal(12)

                // Each unit must appear as one unbroken BEGIN..COMMIT block.
                for (let start = 0; start < relevant.length; start += 4) {
                    const [begin, first, second, commit] = relevant.slice(
                        start,
                        start + 4,
                    )
                    expect(begin).to.equal("BEGIN IMMEDIATE")
                    expect(commit).to.equal("COMMIT")
                    const tag = /'(u\d)-a'/.exec(first)
                    expect(tag, `no tag in ${first}`).to.not.equal(null)
                    expect(second).to.contain(`'${tag![1]}-b'`)
                }
            }),
        )
    })

    it("should roll back a transaction whose COMMIT never reached sqlite", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A failed COMMIT leaves isTransactionActive true and the
                // transaction open in sqlite. Release has to roll it back, or
                // the next runner reads rows the caller was told were lost and
                // its own BEGIN fails for as long as the process lives.
                const log = captureLog(connection)
                connection.subscribers.push({
                    beforeQuery(event: { query: string }) {
                        if (event.query === "COMMIT") {
                            throw new Error("subscriber commit failure")
                        }
                        return undefined
                    },
                } as never)

                const holder = connection.createQueryRunner()
                const next = connection.createQueryRunner()
                let commitFailure = "no error"
                try {
                    await holder.startTransaction()
                    await holder.query(
                        `INSERT INTO thing (name) VALUES ('uncommitted')`,
                    )
                    try {
                        await holder.commitTransaction()
                    } catch (err) {
                        commitFailure = (err as Error).message
                    }
                    expect(commitFailure).to.contain(
                        "subscriber commit failure",
                    )
                    expect(holder.isTransactionActive).to.equal(true)

                    connection.subscribers.length = 0
                    await holder.release()

                    expect(
                        log.getAbandonedTransactionRollbackCount(),
                    ).to.be.greaterThan(0)
                    expect(await next.query(`SELECT name FROM thing`)).to.eql(
                        [],
                    )
                    // A transaction left open fails this with SQLITE_ERROR.
                    await next.startTransaction()
                    await next.commitTransaction()
                } finally {
                    connection.subscribers.length = 0
                    log.restore()
                    await next.release()
                    await holder.release()
                }
            }),
        )
    })

    it("should roll back nested savepoints abandoned by release", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // Releasing at savepoint depth 2 must undo the outer
                // transaction too, and must leave the runner reporting no
                // transaction rather than the depth it abandoned.
                const holder = connection.createQueryRunner()
                const next = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    await holder.query(
                        `INSERT INTO thing (name) VALUES ('outer')`,
                    )
                    await holder.startTransaction()
                    await holder.query(
                        `INSERT INTO thing (name) VALUES ('inner')`,
                    )
                    expect(holder.isTransactionActive).to.equal(true)

                    await holder.release()
                    expect(holder.isTransactionActive).to.equal(false)

                    expect(await next.query(`SELECT name FROM thing`)).to.eql(
                        [],
                    )
                    await next.startTransaction()
                    await next.commitTransaction()
                } finally {
                    await next.release()
                    await holder.release()
                }
            }),
        )
    })

    it("should refuse a statement that races the release of a never-used runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // release() on a runner that never took a lease has nothing to
                // revoke, so it yields with the runner not yet marked
                // released. A first statement in that window was granted the
                // connection, committed in autocommit, and then held the
                // connection for the life of the process, because the release
                // it raced had already passed over the lease it took.
                const runner = connection.createQueryRunner()
                let racingOutcome = "never ran"
                try {
                    const releasing = runner.release()
                    // .then, not await: this statement has to start before the
                    // release above has finished.
                    const racing = runner
                        .query(
                            `INSERT INTO thing (name) VALUES ('after-release')`,
                        )
                        .then(
                            () => "resolved",
                            (err: Error) => err.constructor.name,
                        )
                    await releasing
                    racingOutcome = await racing
                } finally {
                    await runner.release()
                }

                expect(racingOutcome).to.equal(
                    QueryRunnerAlreadyReleasedError.name,
                )

                // The connection was never taken, so it is still free.
                const startedAt = Date.now()
                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => thing.name)
                expect(Date.now() - startedAt).to.be.lessThan(1000)
                expect(names).to.eql([])
            }),
        )
    })

    it("should refuse the holder's next statement after the DataSource is destroyed", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const holder = connection.createQueryRunner()
                let outcome: unknown = "resolved"
                try {
                    await holder.startTransaction()
                    await connection.destroy()

                    // The handle is closed. A driver that drops its pool on
                    // close falls back to handing out the raw connection, and
                    // the holder then reaches a closed handle: better-sqlite3
                    // raises a bare TypeError with no idea what happened.
                    try {
                        await holder.query("SELECT 1")
                    } catch (err) {
                        outcome = err
                    }
                } finally {
                    await connection.initialize()
                }
                expect(outcome).to.be.instanceOf(TypeORMError)
            }),
        )
    })

    it("should serve a new connection after destroy() with a transaction still open", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const abandoned = connection.createQueryRunner()
                await abandoned.startTransaction()
                await abandoned.query(
                    `INSERT INTO thing (name) VALUES ('abandoned')`,
                )

                // The lock must not outlive the handle it was guarding. Beacon
                // destroys and re-initializes the same DataSource on every boot,
                // and a carried-over lock fails the first statement of the new one.
                await connection.destroy()
                await connection.initialize()

                const startedAt = Date.now()
                await connection.query("SELECT 1")
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        )
    })

    it("should refuse a statement held past release by a BeforeQuery subscriber", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // query() checks the runner once, at the top, before its awaits.
                // A subscriber can hold a statement until after release().
                // Without a second check it then runs in the next runner's transaction.
                const sql = `INSERT INTO thing (name) VALUES ('late')`
                let openGate = () => {}
                let reportEntered = () => {}
                const gate = new Promise<void>((ok) => {
                    openGate = ok
                })
                const entered = new Promise<void>((ok) => {
                    reportEntered = ok
                })
                connection.subscribers.push({
                    beforeQuery(event: { query: string }) {
                        if (event.query === sql) {
                            reportEntered()
                            return gate
                        }
                        return undefined
                    },
                } as never)

                const holder = connection.createQueryRunner()
                const next = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    const held = holder.query(sql).then(
                        () => "resolved",
                        (err: Error) => err.constructor.name,
                    )
                    await entered

                    await holder.release()
                    await next.startTransaction()
                    openGate()

                    expect(await held).to.equal(
                        QueryRunnerAlreadyReleasedError.name,
                    )
                    const seenByNext = await next.query(
                        `SELECT name FROM thing`,
                    )
                    expect(seenByNext).to.eql([])
                    await next.rollbackTransaction()
                } finally {
                    openGate()
                    connection.subscribers.length = 0
                    await next.release()
                    await holder.release()
                }

                const names = (
                    await connection.getRepository(Thing).find()
                ).map((thing) => thing.name)
                expect(names).to.eql([])
            }),
        )
    })

    it("should not hand on a connection whose release rollback could not be delivered", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A subscriber can reject ROLLBACK.
                // That used to be swallowed, and the lock was freed anyway.
                // The next runner then read the abandoned row.
                // Its BEGIN failed from then on.
                // The cleanup no longer goes through query(), so it still lands.
                const holder = connection.createQueryRunner()
                const next = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    await holder.query(
                        `INSERT INTO thing (name) VALUES ('abandoned')`,
                    )
                    connection.subscribers.push({
                        beforeQuery(event: { query: string }) {
                            if (event.query === "ROLLBACK") {
                                throw new Error("subscriber rollback failure")
                            }
                            return undefined
                        },
                    } as never)

                    await holder.release()
                    connection.subscribers.length = 0

                    expect(await next.query(`SELECT name FROM thing`)).to.eql(
                        [],
                    )
                    // A transaction left open fails this with SQLITE_ERROR.
                    await next.startTransaction()
                    await next.rollbackTransaction()
                } finally {
                    connection.subscribers.length = 0
                    await next.release()
                    await holder.release()
                }
            }),
        )
    })

    it("should not share the lease between data sources", async () => {
        // A regression to one module-global lease would serialize unrelated databases.
        // It can deadlock an app coordinating two of them.
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
            entities: TEST_ENTITIES,
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
                // The lock is taken in connect(), before any SQL is known, so the
                // message names the cause rather than the statement.
                expect(message).to.contain("query runner was never released")
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        )
    })

    it("should share one acquisition across concurrent statements on one runner", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // A regression to per-statement acquire self-deadlocks:
                // the second statement queues behind its own runner and times out.
                // The in-transaction pair is the case that catches it.
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

    it("should keep the queue in order when a waiting runner is released", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // query() takes its place in line synchronously, so these
                // queue in call order. Releasing the middle one must not move
                // the others or consume the turn it gives up.
                const holder = connection.createQueryRunner()
                const first = connection.createQueryRunner()
                const abandoned = connection.createQueryRunner()
                const last = connection.createQueryRunner()
                try {
                    await holder.startTransaction()

                    const order: string[] = []
                    // .then, not await: all three have to be queued at once.
                    const firstDone = first.query("SELECT 1").then(() => {
                        order.push("first")
                    })
                    const abandonedDone = abandoned.query("SELECT 2").then(
                        () => {
                            order.push("abandoned")
                        },
                        () => {
                            order.push("abandoned-refused")
                        },
                    )
                    const lastDone = last.query("SELECT 3").then(() => {
                        order.push("last")
                    })

                    await abandoned.release()
                    await abandonedDone
                    await holder.rollbackTransaction()
                    await holder.release()

                    await firstDone
                    await first.release()
                    await lastDone

                    expect(order).to.eql(["abandoned-refused", "first", "last"])
                } finally {
                    await last.release()
                    await abandoned.release()
                    await first.release()
                    await holder.release()
                }
            }),
        )
    })

    it("should fail fast on a runner whose wait for the connection already timed out", () => {
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
                    // The holder keeps the connection until it is released,
                    // not until its transaction commits.
                    await holder.release()

                    // A runner takes one lease and never another, so this
                    // reports the wait that already failed instead of starting
                    // a second one. Waiting again would let one runner spend
                    // the acquire deadline over and over, and no caller retries
                    // a statement on a runner that already threw.
                    const startedAt = Date.now()
                    await waiter
                        .query("SELECT 1")
                        .should.be.rejectedWith(/Timed out after \d+ms/)
                    expect(Date.now() - startedAt).to.be.lessThan(400)
                } finally {
                    await waiter.release()
                    await holder.release()
                }

                // Only that runner is spent. The connection is free.
                const startedAt = Date.now()
                await connection.query("SELECT 1")
                expect(Date.now() - startedAt).to.be.lessThan(400)
            }),
        )
    })
})

describe("sqlite driver > query runner ownership > result cache", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: TEST_ENTITIES,
            enabledDrivers: SQLITE_DRIVERS,
            cache: true,
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

    it("should free the connection after queryResultCache.clear()", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // clear() makes its own query runner when the caller passes none.
                // It has to release it.
                // Forgetting left nothing for any later query to use.
                await connection.queryResultCache!.clear(undefined as never)

                const startedAt = Date.now()
                expect(await connection.query("SELECT 1 AS value")).to.eql([
                    { value: 1 },
                ])
                expect(Date.now() - startedAt).to.be.lessThan(500)
            }),
        )
    })
})

describe("sqlite driver > query runner ownership > unusable connection", () => {
    let connections: DataSource[]
    // Its own connections: refusing one is permanent for that DataSource, so this
    // test cannot share a fixture with the others.
    before(async () => {
        connections = await createTestingConnections({
            entities: TEST_ENTITIES,
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: { connectionLeaseTimeout: 500 },
        })
        expectBothSqliteDrivers(connections)
        await reloadTestingDatabases(connections)
    })
    after(() => {
        return closeTestingConnections(connections)
    })

    it("should refuse the connection when the rollback cannot be delivered", () => {
        return Promise.all(
            connections.map(async (connection) => {
                // The rollback runs straight on the connection, so almost nothing can
                // stop it. If sqlite itself refuses, we no longer know whether a
                // transaction is open.
                // Handing that connection on is worse than failing loudly.
                // Each driver is broken the way its own library reports failure.
                const handle = (
                    connection.driver as unknown as {
                        databaseConnection: Record<string, unknown>
                    }
                ).databaseConnection
                const busy = Object.assign(new Error("database is locked"), {
                    code: "SQLITE_BUSY",
                })
                const isNodeSqlite = connection.options.type === "sqlite"
                const brokenMethod = isNodeSqlite ? "run" : "exec"
                const realMethod = handle[brokenMethod]

                const holder = connection.createQueryRunner()
                try {
                    await holder.startTransaction()
                    handle[brokenMethod] = (
                        sql: string,
                        callback?: (err: unknown) => void,
                    ) => {
                        if (sql !== "ROLLBACK") {
                            return (
                                realMethod as (...args: unknown[]) => unknown
                            ).call(handle, sql, callback)
                        }
                        if (callback) {
                            callback(busy)
                            return undefined
                        }
                        throw busy
                    }
                    await holder.release()
                } finally {
                    handle[brokenMethod] = realMethod
                }

                const next = connection.createQueryRunner()
                let message = "no error"
                try {
                    await next.query(`SELECT name FROM thing`)
                } catch (err) {
                    message = (err as Error).message
                }
                expect(message).to.contain("abandoned transaction")
                expect(message).to.contain("database is locked")
            }),
        )
    })
})

describe("sqlite driver > query runner ownership > driver without leasing", () => {
    let connection: DataSource
    before(async () => {
        // sqljs is one of the six sqlite drivers the fork leaves alone. It is
        // the only place the unleased branches of connect() and release() run,
        // and the PR claims those drivers are untouched.
        connection = new DataSource({ type: "sqljs", autoSave: false })
        await connection.initialize()
        await connection.query(`CREATE TABLE item (name TEXT)`)
    })
    after(() => {
        return connection.destroy()
    })

    it("should keep a released runner usable, as upstream does", async () => {
        const runner = connection.createQueryRunner()
        await runner.startTransaction()
        await runner.query(`INSERT INTO item (name) VALUES ('kept')`)
        await runner.commitTransaction()

        // Upstream's sqlite release() is a memory reset, not a lifecycle end.
        // A runner is reusable afterwards and never reports itself released.
        await runner.release()
        expect(runner.isReleased).to.equal(false)

        expect(await runner.query(`SELECT name FROM item`)).to.eql([
            { name: "kept" },
        ])
        await runner.connect()
        await runner.release()
    })

    it("should let two runners share the connection, as upstream does", async () => {
        // Without leasing there is nothing to serialize: a second runner is
        // handed the same connection with no wait and no deadline.
        const first = connection.createQueryRunner()
        const second = connection.createQueryRunner()
        try {
            const startedAt = Date.now()
            expect(await first.connect()).to.equal(await second.connect())
            expect(Date.now() - startedAt).to.be.lessThan(1000)
        } finally {
            await second.release()
            await first.release()
        }
    })
})
