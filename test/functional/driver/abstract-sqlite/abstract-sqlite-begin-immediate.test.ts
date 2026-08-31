import "reflect-metadata"
import { expect } from "chai"
import { DataSource } from "../../../../src"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import { Thing } from "./entity/Thing"
import {
    captureSql,
    expectBothSqliteDrivers,
    openSecondHandle,
    SQLITE_DRIVERS,
    TEST_ENTITIES,
} from "./sqlite-lease-test-utils"

/**
 * Why the driver rewrites BEGIN to BEGIN IMMEDIATE.
 *
 * Another connection can write the same database, such as a sync engine in a worker thread.
 * The lease cannot serialize a writer it does not manage.
 * Under write-ahead logging (WAL),
 * a deferred BEGIN pins a read snapshot on the first read.
 * If another connection commits before our upgrade to writer,
 * the upgrade fails with SQLITE_BUSY_SNAPSHOT.
 * No retry can clear that state, and the unit of work is left half done.
 *
 * BEGIN IMMEDIATE takes the write lock up front.
 * The loser is then whoever arrives second, at BEGIN, having done nothing,
 * and it fails with a plain SQLITE_BUSY that retries cleanly.
 */
describe("sqlite driver > begin immediate", () => {
    let connections: DataSource[]
    before(async () => {
        connections = await createTestingConnections({
            entities: TEST_ENTITIES,
            enabledDrivers: SQLITE_DRIVERS,
            driverSpecific: { enableWAL: true },
        })
        expectBothSqliteDrivers(connections)
    })
    beforeEach(() => {
        return reloadTestingDatabases(connections)
    })
    after(async () => {
        // WAL is a property of the file, so leave it as the other suites expect to find it.
        for (const connection of connections) {
            await connection.query("PRAGMA journal_mode = DELETE")
        }
        await closeTestingConnections(connections)
    })

    it("should open transactions with BEGIN IMMEDIATE", () => {
        return Promise.all(
            connections.map(async (connection) => {
                const sql = captureSql(connection)
                try {
                    await connection.transaction(async (manager) => {
                        await manager.save(Thing, { name: "written" })
                    })

                    const control = sql.getTransactionControlStatements()
                    expect(
                        control.filter((s) => {
                            return /^\s*BEGIN IMMEDIATE/i.test(s)
                        }),
                    ).to.have.length(1)
                    // The rewrite is keyed off upstream's exact literal.
                    // If upstream ever changes the literal,
                    // this assertion reports that the rewrite stopped firing.
                    expect(
                        control.filter((s) => {
                            return /^\s*BEGIN TRANSACTION/i.test(s)
                        }),
                    ).to.have.length(0)
                } finally {
                    sql.restore()
                }
            }),
        )
    })

    it("should complete a read-then-write transaction while another writer contends", () => {
        return Promise.all(
            connections.map(async (connection) => {
                let competingWriterCode = "committed"

                await connection.transaction(async (manager) => {
                    // Reading first is what pins a snapshot under a deferred BEGIN.
                    await manager.getRepository(Thing).count()

                    const other = await openSecondHandle(connection)
                    try {
                        await other.exec(
                            `INSERT INTO thing (name) VALUES ('competing')`,
                        )
                    } catch (err) {
                        const shape = err as {
                            code?: string
                            message?: string
                        }
                        competingWriterCode =
                            shape.code ?? shape.message ?? "unknown"
                    } finally {
                        await other.close()
                    }

                    await manager.save(Thing, { name: "mine" })
                })

                // We hold the write lock for the whole transaction,
                // so the other writer is the one turned away,
                // and with a code that is safe to retry.
                expect(competingWriterCode).to.match(/^SQLITE_BUSY$/)

                const names = (await connection.getRepository(Thing).find())
                    .map((thing) => {
                        return thing.name
                    })
                    .sort()
                expect(names).to.eql(["mine"])
            }),
        )
    })
})
