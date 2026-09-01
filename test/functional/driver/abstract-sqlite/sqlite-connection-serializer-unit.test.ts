import { expect } from "chai"
import { AbstractSqliteDriver } from "../../../../src/driver/sqlite-abstract/AbstractSqliteDriver"
import { SqliteConnectionLease } from "../../../../src/driver/sqlite-abstract/SqliteConnectionLease"
import { SqliteConnectionSerializer } from "../../../../src/driver/sqlite-abstract/SqliteConnectionSerializer"
import { SqliteLeaseOptions } from "../../../../src/driver/sqlite-abstract/sqlite.types"

/**
 * Unit tests for the serialization machinery. No sqlite involved.
 */

/** Long enough for any in-memory promise chain to settle, short enough to keep the suite fast. */
const SETTLE_MS = 50

const busyError = {
    code: "SQLITE_BUSY",
    message: "SQLITE_BUSY: database is locked",
}

/**
 * The whole surface the serializer reads off its driver,
 * plus the warnings its logger collected.
 */
interface StubDriver {
    options: SqliteLeaseOptions
    connection: {
        logger: {
            log: (level: string, message: unknown) => void
        }
    }
    warnings: string[]
}

/**
 * Each call makes a fresh driver, so each test gets its own connection lease.
 */
function createStubDriver(
    options: SqliteLeaseOptions = {},
): AbstractSqliteDriver {
    const warnings: string[] = []
    const driver: StubDriver = {
        options,
        warnings,
        connection: {
            logger: {
                log: (level, message) => {
                    if (level === "warn" && typeof message === "string") {
                        warnings.push(message)
                    }
                },
            },
        },
    }
    // Deliberately partial: the cast confines the untyped surface to this helper.
    return driver as unknown as AbstractSqliteDriver
}

function getWarnings(driver: AbstractSqliteDriver): string[] {
    return (driver as unknown as StubDriver).warnings
}

/** An executeStatement stub that succeeds and records the sql it was handed. */
function createRecordingExecute(): {
    executeStatement: (sql: string) => Promise<string>
    getStatements: () => string[]
} {
    const statements: string[] = []
    return {
        executeStatement: async (sql: string) => {
            statements.push(sql)
            return "ok"
        },
        getStatements: () => {
            return statements
        },
    }
}

/**
 * An executeStatement stub that rejects with the given error the first failureCount
 * calls, then succeeds.
 */
function createFailingExecute(
    failureCount: number,
    error: unknown = busyError,
): {
    executeStatement: (sql: string) => Promise<string>
    getCallCount: () => number
} {
    let callCount = 0
    return {
        executeStatement: (sql: string) => {
            callCount += 1
            if (callCount <= failureCount) {
                return Promise.reject(error)
            }
            return Promise.resolve(sql)
        },
        getCallCount: () => {
            return callCount
        },
    }
}

async function sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
        setTimeout(resolve, ms)
    })
}

/**
 * A statement whose completion the test controls,
 * so a statement can be held in flight while other things are asserted.
 */
function createGatedExecute(): {
    executeStatement: () => Promise<string>
    finish: () => void
} {
    let finish!: () => void
    const gate = new Promise<string>((resolve) => {
        finish = () => {
            resolve("ok")
        }
    })
    return {
        executeStatement: () => {
            return gate
        },
        finish,
    }
}

/**
 * True when a fresh serializer on the same driver can take the connection.
 * A probe that stays queued proves the connection is still held.
 */
async function isConnectionFree(
    driver: AbstractSqliteDriver,
): Promise<boolean> {
    const probe = new SqliteConnectionSerializer(driver)
    let hasRun = false
    const pending = probe
        .run("SELECT 1", async () => {
            hasRun = true
            return "probe"
        })
        .catch(() => {
            // A probe that never gets the connection times out long after the test.
        })
    await Promise.race([pending, sleep(SETTLE_MS)])
    return hasRun
}

/** Opens a transaction the only way the serializer learns about one. */
async function openTransaction(
    serializer: SqliteConnectionSerializer,
): Promise<void> {
    await serializer.run("BEGIN TRANSACTION", async () => {
        return "ok"
    })
}

describe("sqlite driver > connection lease", () => {
    it("should grant immediately when free", async () => {
        const lease = new SqliteConnectionLease()
        await lease.acquire("A", 100)
        expect(lease.currentlyRunningSql).to.equal("A")
    })

    it("should hand the lease to waiters in FIFO order", async () => {
        const lease = new SqliteConnectionLease()
        await lease.acquire("A", 100)

        // .then instead of await:
        // awaiting would block this test on the very grants whose order it records.
        const order: string[] = []
        const b = lease.acquire("B", 1000).then(() => {
            order.push("B")
        })
        const c = lease.acquire("C", 1000).then(() => {
            order.push("C")
        })
        expect(lease.queueLength).to.equal(2)

        lease.release()
        await b
        lease.release()
        await c
        expect(order).to.eql(["B", "C"])
    })

    it("should time out with a diagnostic naming both sides and remove the waiter", async () => {
        const lease = new SqliteConnectionLease()
        await lease.acquire("HOLDER SQL", 100)

        let message = "no error"
        try {
            await lease.acquire("WAITER SQL", 20)
        } catch (err) {
            message = (err as Error).message
        }
        expect(message).to.contain("Waiting to run: WAITER SQL")
        expect(message).to.contain("Blocked by: HOLDER SQL")
        expect(lease.queueLength).to.equal(0)
    })

    it("should not let a new arrival overtake a woken waiter", async () => {
        const lease = new SqliteConnectionLease()
        await lease.acquire("A", 100)

        // .then instead of await:
        // awaiting would serialize the concurrency this test measures.
        const order: string[] = []
        const b = lease.acquire("B", 1000).then(() => {
            order.push("B")
        })
        lease.release()
        // B is granted but has not resumed yet, so D must queue behind it.
        const d = lease.acquire("D", 1000).then(() => {
            order.push("D")
        })

        await b
        lease.release()
        await d
        expect(order).to.eql(["B", "D"])
    })

    it("should free the lease when released with no waiters", async () => {
        const lease = new SqliteConnectionLease()
        await lease.acquire("A", 100)
        lease.release()

        expect(lease.currentlyRunningSql).to.equal(undefined)
        await lease.acquire("B", 100)
        expect(lease.currentlyRunningSql).to.equal("B")
    })
})

describe("sqlite driver > connection serializer > connection lifetime", () => {
    it("should free the connection after a plain statement", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await serializer.run("SELECT 1", async () => {
            return "ok"
        })

        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should free the connection when a statement throws", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        let hasFailed = false
        try {
            await serializer.run("SELECT 1", async () => {
                throw new Error("no such table")
            })
        } catch {
            hasFailed = true
        }

        expect(hasFailed).to.equal(true)
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should hold the connection from BEGIN until COMMIT", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await openTransaction(serializer)
        expect(await isConnectionFree(driver)).to.equal(false)

        await serializer.run("COMMIT", async () => {
            return "ok"
        })
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should hold the connection across statements inside a transaction", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await openTransaction(serializer)
        await serializer.run(
            "INSERT INTO thing (name) VALUES ('x')",
            async () => {
                return "ok"
            },
        )

        // The statement finished, but the transaction did not.
        expect(await isConnectionFree(driver)).to.equal(false)

        await serializer.run("ROLLBACK", async () => {
            return "ok"
        })
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should share one acquire across concurrent statements on one runner", async () => {
        const driver = createStubDriver({ connectionLeaseTimeout: 500 })
        const serializer = new SqliteConnectionSerializer(driver)

        // A per-statement acquire would make the second statement queue behind
        // the connection its own runner already holds, and time out.
        const results = await Promise.all([
            serializer.run("SELECT 1", async () => {
                return "first"
            }),
            serializer.run("SELECT 2", async () => {
                return "second"
            }),
        ])

        expect(results).to.eql(["first", "second"])
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should hold the connection until every in-flight statement finishes", async () => {
        // Freeing it when the first of several concurrent statements finishes
        // would let the next runner open a transaction around the ones still
        // executing, which is the lost-write bug through another door.
        const driver = createStubDriver()
        const first = new SqliteConnectionSerializer(driver)
        const second = new SqliteConnectionSerializer(driver)

        const early = createGatedExecute()
        const late = createGatedExecute()
        const earlyStatement = first.run("SELECT 1", early.executeStatement)
        const lateStatement = first.run("SELECT 2", late.executeStatement)

        let hasSecondRunnerRun = false
        const queued = second.run("SELECT 3", async () => {
            hasSecondRunnerRun = true
            return "ok"
        })

        early.finish()
        await earlyStatement
        await sleep(SETTLE_MS)

        // One statement is still in flight, so the connection must stay held.
        expect(hasSecondRunnerRun).to.equal(false)

        late.finish()
        await lateStatement
        await queued
        expect(hasSecondRunnerRun).to.equal(true)
    })

    it("should make a second runner wait while the first holds a transaction", async () => {
        const driver = createStubDriver()
        const first = new SqliteConnectionSerializer(driver)
        const second = new SqliteConnectionSerializer(driver)

        await openTransaction(first)

        let hasSecondRun = false
        const pending = second.run("SELECT 1", async () => {
            hasSecondRun = true
            return "ok"
        })

        await sleep(SETTLE_MS)
        expect(hasSecondRun).to.equal(false)

        await first.run("COMMIT", async () => {
            return "ok"
        })
        await pending
        expect(hasSecondRun).to.equal(true)
    })

    it("should let the next statement retry after a lease timeout", async () => {
        const driver = createStubDriver({ connectionLeaseTimeout: 20 })
        const holder = new SqliteConnectionSerializer(driver)
        const waiter = new SqliteConnectionSerializer(driver)

        await openTransaction(holder)

        let message = "no error"
        try {
            await waiter.run("SELECT 1", async () => {
                return "ok"
            })
        } catch (err) {
            message = (err as Error).message
        }
        expect(message).to.contain("Timed out after 20ms")

        await holder.run("COMMIT", async () => {
            return "ok"
        })

        // The failed acquire must not be cached on the runner.
        const result = await waiter.run("SELECT 1", async () => {
            return "second attempt"
        })
        expect(result).to.equal("second attempt")
    })
})

describe("sqlite driver > connection serializer > transaction tracking", () => {
    it("should report no open transaction before any statement", () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())
        expect(serializer.isTransactionOpen).to.equal(false)
    })

    const openingStatements = [
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN IMMEDIATE",
        "BEGIN DEFERRED",
        "BEGIN EXCLUSIVE",
        "  begin immediate  ",
        "BEGIN;",
    ]

    for (const statement of openingStatements) {
        it(`should treat ${JSON.stringify(
            statement,
        )} as opening a transaction`, async () => {
            const serializer = new SqliteConnectionSerializer(
                createStubDriver(),
            )
            await serializer.run(statement, async () => {
                return "ok"
            })
            expect(serializer.isTransactionOpen).to.equal(true)
        })
    }

    const closingStatements = [
        "COMMIT",
        "END",
        "ROLLBACK",
        "commit;",
        "END TRANSACTION",
    ]

    for (const statement of closingStatements) {
        it(`should treat ${JSON.stringify(
            statement,
        )} as closing a transaction`, async () => {
            const serializer = new SqliteConnectionSerializer(
                createStubDriver(),
            )
            await openTransaction(serializer)
            await serializer.run(statement, async () => {
                return "ok"
            })
            expect(serializer.isTransactionOpen).to.equal(false)
        })
    }

    it("should not treat ROLLBACK TO SAVEPOINT as closing the transaction", async () => {
        // Freeing the connection here would hand it over mid-transaction.
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await openTransaction(serializer)
        await serializer.run("ROLLBACK TO SAVEPOINT typeorm_1", async () => {
            return "ok"
        })

        expect(serializer.isTransactionOpen).to.equal(true)
        expect(await isConnectionFree(driver)).to.equal(false)
    })

    it("should not treat SAVEPOINT or RELEASE SAVEPOINT as transaction control", async () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())

        await serializer.run("SAVEPOINT typeorm_1", async () => {
            return "ok"
        })
        expect(serializer.isTransactionOpen).to.equal(false)

        await openTransaction(serializer)
        await serializer.run("RELEASE SAVEPOINT typeorm_1", async () => {
            return "ok"
        })
        expect(serializer.isTransactionOpen).to.equal(true)
    })

    it("should not open a transaction when the BEGIN fails", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        let hasFailed = false
        try {
            await serializer.run("BEGIN TRANSACTION", async () => {
                throw new Error("database is locked")
            })
        } catch {
            hasFailed = true
        }

        expect(hasFailed).to.equal(true)
        expect(serializer.isTransactionOpen).to.equal(false)
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should keep the transaction open when the COMMIT fails", async () => {
        // The transaction is still open in sqlite, so the connection must stay held
        // until release() rolls it back.
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await openTransaction(serializer)
        let hasFailed = false
        try {
            await serializer.run("COMMIT", async () => {
                throw new Error("database is locked")
            })
        } catch {
            hasFailed = true
        }

        expect(hasFailed).to.equal(true)
        expect(serializer.isTransactionOpen).to.equal(true)
        expect(await isConnectionFree(driver)).to.equal(false)
    })

    it("should track a raw BEGIN with no help from the query runner", async () => {
        // The serializer never sees runner flags, so a raw query("BEGIN") needs no special case.
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await serializer.run("BEGIN", async () => {
            return "ok"
        })

        expect(serializer.isTransactionOpen).to.equal(true)
        expect(await isConnectionFree(driver)).to.equal(false)
    })
})

describe("sqlite driver > connection serializer > rollback on release", () => {
    it("should roll back an open transaction and free the connection", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)
        const { executeStatement, getStatements } = createRecordingExecute()

        await openTransaction(serializer)
        await serializer.rollbackOnRelease(executeStatement)

        expect(getStatements()).to.eql(["ROLLBACK"])
        expect(serializer.isTransactionOpen).to.equal(false)
        expect(await isConnectionFree(driver)).to.equal(true)
        expect(getWarnings(driver)[0]).to.contain(
            "released with a transaction still open",
        )
    })

    it("should do nothing when no transaction is open", async () => {
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)
        const { executeStatement, getStatements } = createRecordingExecute()

        await serializer.rollbackOnRelease(executeStatement)

        expect(getStatements()).to.eql([])
        expect(getWarnings(driver)).to.eql([])
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should free the connection when the rollback itself fails", async () => {
        // Otherwise the transaction stays marked open, the connection is held
        // for the life of the driver, and every later runner times out.
        const driver = createStubDriver()
        const serializer = new SqliteConnectionSerializer(driver)

        await openTransaction(serializer)
        await serializer.rollbackOnRelease(async () => {
            throw new Error("disk I/O error")
        })

        expect(serializer.isTransactionOpen).to.equal(false)
        expect(await isConnectionFree(driver)).to.equal(true)
    })

    it("should never throw, so it cannot mask the error that caused the release", async () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())

        await openTransaction(serializer)
        // Resolving rather than rejecting is the assertion.
        await serializer.rollbackOnRelease(async () => {
            throw new Error("disk I/O error")
        })
    })
})

describe("sqlite driver > connection serializer > begin immediate", () => {
    it("should rewrite BEGIN TRANSACTION to BEGIN IMMEDIATE", async () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())
        const { executeStatement, getStatements } = createRecordingExecute()

        await serializer.run("BEGIN TRANSACTION", executeStatement)

        expect(getStatements()).to.eql(["BEGIN IMMEDIATE"])
    })

    it("should pass every other statement through unchanged", async () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())
        const { executeStatement, getStatements } = createRecordingExecute()

        await serializer.run("SELECT 1", executeStatement)
        await serializer.run("BEGIN", executeStatement)
        await serializer.run("COMMIT", executeStatement)

        expect(getStatements()).to.eql(["SELECT 1", "BEGIN", "COMMIT"])
    })
})

describe("sqlite driver > connection serializer > busy retry", () => {
    const retryOptions: SqliteLeaseOptions = {
        busyErrorRetryInterval: 1,
        busyErrorRetryTimeout: 200,
    }

    it("should not retry when busyErrorRetryInterval is unset", async () => {
        const serializer = new SqliteConnectionSerializer(createStubDriver())
        const { executeStatement, getCallCount } = createFailingExecute(99)

        let hasFailed = false
        try {
            await serializer.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    it("should not retry when busyErrorRetryTimeout is 0", async () => {
        const serializer = new SqliteConnectionSerializer(
            createStubDriver({
                busyErrorRetryInterval: 1,
                busyErrorRetryTimeout: 0,
            }),
        )
        const { executeStatement, getCallCount } = createFailingExecute(99)

        let hasFailed = false
        try {
            await serializer.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    it("should retry a busy statement until it succeeds", async () => {
        const driver = createStubDriver(retryOptions)
        const serializer = new SqliteConnectionSerializer(driver)
        const { executeStatement, getCallCount } = createFailingExecute(2)

        await serializer.run("UPDATE thing SET name = 'x'", executeStatement)

        expect(getCallCount()).to.equal(3)
        expect(getWarnings(driver).length).to.equal(2)
        expect(getWarnings(driver)[0]).to.contain(
            "SQLITE_BUSY, retrying in 1ms",
        )
    })

    it("should stop retrying at the wall-clock deadline and surface the sqlite error", async () => {
        const serializer = new SqliteConnectionSerializer(
            createStubDriver({
                busyErrorRetryInterval: 1,
                busyErrorRetryTimeout: 60,
            }),
        )
        const { executeStatement, getCallCount } = createFailingExecute(9999)

        const startedAt = Date.now()
        let failure: unknown
        try {
            await serializer.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch (err) {
            failure = err
        }

        expect(failure).to.equal(busyError)
        expect(getCallCount()).to.be.greaterThan(1)
        expect(Date.now() - startedAt).to.be.greaterThanOrEqual(60)
        expect(Date.now() - startedAt).to.be.lessThan(5000)
    })

    it("should not retry a plain statement inside an open transaction", async () => {
        // Sqlite already rolled the failed statement back,
        // so retrying it alone would commit a partial unit of work.
        const serializer = new SqliteConnectionSerializer(
            createStubDriver(retryOptions),
        )
        await openTransaction(serializer)
        const { executeStatement, getCallCount } = createFailingExecute(99)

        let hasFailed = false
        try {
            await serializer.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    for (const statement of ["COMMIT", "ROLLBACK", "END"]) {
        it(`should retry ${statement} inside an open transaction`, async () => {
            // Both must land, or the transaction left open fails every later BEGIN.
            const serializer = new SqliteConnectionSerializer(
                createStubDriver(retryOptions),
            )
            await openTransaction(serializer)
            const { executeStatement, getCallCount } = createFailingExecute(2)

            await serializer.run(statement, executeStatement)
            expect(getCallCount()).to.equal(3)
        })
    }

    it("should not retry ROLLBACK TO SAVEPOINT inside a transaction", async () => {
        // It does not close the transaction, so it is a plain statement for retry purposes.
        const serializer = new SqliteConnectionSerializer(
            createStubDriver(retryOptions),
        )
        await openTransaction(serializer)
        const { executeStatement, getCallCount } = createFailingExecute(99)

        let hasFailed = false
        try {
            await serializer.run(
                "ROLLBACK TO SAVEPOINT typeorm_1",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    const busyShapes: [string, unknown][] = [
        ["a code property", { code: "SQLITE_BUSY" }],
        ["an extended code property", { code: "SQLITE_BUSY_SNAPSHOT" }],
        [
            "a wrapped driverError code",
            { driverError: { code: "SQLITE_BUSY_RECOVERY" } },
        ],
        ["only a message", new Error("SQLITE_BUSY: database is locked")],
    ]

    for (const [shape, error] of busyShapes) {
        it(`should retry a busy error carrying ${shape}`, async () => {
            const serializer = new SqliteConnectionSerializer(
                createStubDriver(retryOptions),
            )
            const { executeStatement, getCallCount } = createFailingExecute(
                1,
                error,
            )

            await serializer.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
            expect(getCallCount()).to.equal(2)
        })
    }

    const otherErrors: [string, unknown][] = [
        [
            "another sqlite code",
            { code: "SQLITE_ERROR", message: "no such table: thing" },
        ],
        ["an unrelated message", new Error("database disk image is malformed")],
    ]

    for (const [shape, error] of otherErrors) {
        it(`should not retry ${shape}`, async () => {
            const serializer = new SqliteConnectionSerializer(
                createStubDriver(retryOptions),
            )
            const { executeStatement, getCallCount } = createFailingExecute(
                1,
                error,
            )

            let hasFailed = false
            try {
                await serializer.run(
                    "UPDATE thing SET name = 'x'",
                    executeStatement,
                )
            } catch {
                hasFailed = true
            }
            expect(hasFailed).to.equal(true)
            expect(getCallCount()).to.equal(1)
        })
    }
})
