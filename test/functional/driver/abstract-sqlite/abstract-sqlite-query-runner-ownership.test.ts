import "reflect-metadata"
import { expect } from "chai"
import { DataSource } from "../../../../src"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import { Thing } from "./entity/Thing"
import { captureLog, captureSql } from "./sqlite-lease-test-utils"

const SQLITE_DRIVERS: ("sqlite" | "better-sqlite3")[] = [
    "sqlite",
    "better-sqlite3",
]

describe("sqlite driver > query runner ownership", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [__dirname + "/entity/*{.js,.ts}"],
                enabledDrivers: SQLITE_DRIVERS,
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should hand every caller its own query runner", () =>
        Promise.all(
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
        ))

    it("should keep a committed transaction when a concurrent one rolls back", () =>
        Promise.all(
            connections.map(async (connection) => {
                const sql = captureSql(connection)
                try {
                    const results = await Promise.allSettled([
                        connection.transaction(async (manager) => {
                            await manager.save(Thing, { name: "rolled-back" })
                            await new Promise((ok) => setTimeout(ok, 30))
                            throw new Error("ordinary app failure")
                        }),
                        connection.transaction(async (manager) => {
                            await new Promise((ok) => setTimeout(ok, 10))
                            await manager.save(Thing, { name: "committed" })
                        }),
                    ])

                    expect(results[0].status).to.equal("rejected")
                    expect(results[1].status).to.equal("fulfilled")

                    const names = (
                        await connection.getRepository(Thing).find()
                    ).map((thing) => thing.name)

                    // Before the lease both units of work shared one runner, so the second
                    // one's insert became a savepoint inside the first one's transaction and
                    // died with its ROLLBACK -- while its caller was told it had succeeded.
                    expect(names).to.eql(["committed"])

                    const control = sql.transactionControl()
                    expect(
                        control.filter((s) => /^\s*BEGIN IMMEDIATE/i.test(s)),
                    ).to.have.length(2)
                    expect(
                        control.filter((s) => /SAVEPOINT typeorm_/i.test(s)),
                    ).to.have.length(0)
                } finally {
                    sql.restore()
                }
            }),
        ))

    it("should still use savepoints for a real nested transaction on one runner", () =>
        Promise.all(
            connections.map(async (connection) => {
                const sql = captureSql(connection)
                try {
                    await connection.transaction(async (manager) => {
                        const runner = manager.queryRunner!
                        await runner.startTransaction()
                        await manager.save(Thing, { name: "nested" })
                        await runner.commitTransaction()
                    })

                    const control = sql.transactionControl()
                    expect(
                        control.filter((s) =>
                            /^\s*SAVEPOINT typeorm_1/i.test(s),
                        ),
                    ).to.have.length(1)
                    expect(
                        control.filter((s) =>
                            /^\s*RELEASE SAVEPOINT typeorm_1/i.test(s),
                        ),
                    ).to.have.length(1)
                } finally {
                    sql.restore()
                }
            }),
        ))

    it("should not let another runner read a transaction's uncommitted rows", () =>
        Promise.all(
            connections.map(async (connection) => {
                let seenByOther: string[] = []

                await Promise.allSettled([
                    connection.transaction(async (manager) => {
                        await manager.save(Thing, { name: "uncommitted" })
                        await new Promise((ok) => setTimeout(ok, 60))
                        throw new Error("ordinary app failure")
                    }),
                    (async () => {
                        await new Promise((ok) => setTimeout(ok, 20))
                        seenByOther = (
                            await connection.getRepository(Thing).find()
                        ).map((thing) => thing.name)
                    })(),
                ])

                // Sqlite has one connection, so any statement issued while a transaction is
                // open is inside it. The reader has to wait rather than join.
                expect(seenByOther).to.not.include("uncommitted")
            }),
        ))

    it("should roll back and free the connection when a runner is released mid-transaction", () =>
        Promise.all(
            connections.map(async (connection) => {
                const log = captureLog(connection)
                try {
                    const abandoned = connection.createQueryRunner()
                    await abandoned.startTransaction()
                    await abandoned.manager.save(Thing, { name: "abandoned" })
                    await abandoned.release()

                    expect(log.leakRecoveries()).to.be.greaterThan(0)

                    // The connection is usable again, and the abandoned work is gone.
                    const names = (
                        await connection.getRepository(Thing).find()
                    ).map((thing) => thing.name)
                    expect(names).to.eql([])
                } finally {
                    log.restore()
                }
            }),
        ))

    it("should serialize an implicit save() transaction against an explicit one", () =>
        Promise.all(
            connections.map(async (connection) => {
                // EntityPersistExecutor opens its own transaction, so a bare save() is a unit
                // of work in its own right, not a statement that can join someone else's.
                const results = await Promise.allSettled([
                    connection.transaction(async (manager) => {
                        await manager.save(Thing, { name: "explicit" })
                        await new Promise((ok) => setTimeout(ok, 30))
                    }),
                    connection.getRepository(Thing).save({ name: "implicit" }),
                ])

                expect(results.map((r) => r.status)).to.eql([
                    "fulfilled",
                    "fulfilled",
                ])

                const names = (await connection.getRepository(Thing).find())
                    .map((thing) => thing.name)
                    .sort()
                expect(names).to.eql(["explicit", "implicit"])
            }),
        ))
})

describe("sqlite driver > query runner ownership > lease timeout", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [__dirname + "/entity/*{.js,.ts}"],
                enabledDrivers: SQLITE_DRIVERS,
                driverSpecific: { connectionLeaseTimeout: 500 },
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should fail with a diagnostic rather than hang when a runner waits on its own caller", () =>
        Promise.all(
            connections.map(async (connection) => {
                const startedAt = Date.now()
                let message = "no error"

                try {
                    await connection.transaction(async () => {
                        // Reaching past the transaction's own manager for a fresh runner is
                        // the shape of the bug in cloud-connector's doWithQueryRunner.
                        const independent = connection.createQueryRunner()
                        try {
                            await independent.query("SELECT 1")
                        } finally {
                            await independent.release()
                        }
                    })
                } catch (err) {
                    message = (err as Error).message
                }

                expect(message).to.match(
                    /Timed out after \d+ms waiting for the sqlite connection/,
                )
                // Names both sides, so the offending call site is identifiable from the log.
                expect(message).to.contain("Waiting on: SELECT 1")
                expect(message).to.contain("Holder was running:")
                expect(Date.now() - startedAt).to.be.lessThan(5000)
            }),
        ))
})
