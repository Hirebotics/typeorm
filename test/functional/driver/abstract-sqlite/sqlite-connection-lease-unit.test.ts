import { expect } from "chai"
import {
    SqliteConnectionLease,
    SqliteLeaseHolder,
} from "../../../../src/driver/sqlite-abstract/SqliteConnectionLease"
import { SqliteLeasedQueryRunner } from "../../../../src/driver/sqlite-abstract/sqlite.types"

/**
 * Pure unit tests for the lease machinery. No sqlite involved.
 */
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

/**
 * The smallest object that satisfies what SqliteLeaseHolder reads off its runner.
 * Each call makes a fresh driver object, so each test gets its own lease.
 * Deliberately partial: the cast confines the untyped surface to this one helper.
 */
function createStubRunner(
    overrides: Record<string, unknown> = {},
): SqliteLeasedQueryRunner {
    return {
        driver: {
            options: {
                busyErrorRetryInterval: 1,
                busyErrorRetryTimeout: 100,
            },
        },
        connection: {
            logger: {
                log: () => {
                    return undefined
                },
            },
        },
        isTransactionActive: false,
        isReleased: false,
        transactionDepth: 0,
        ...overrides,
    } as unknown as SqliteLeasedQueryRunner
}

describe("sqlite driver > connection lease > busy retry gate", () => {
    /**
     * Makes an executeStatement stub.
     * It fails with SQLITE_BUSY the first failureCount calls, then succeeds.
     */
    function createBusyFailingExecute(failureCount: number): {
        executeStatement: () => Promise<string>
        getCallCount: () => number
    } {
        let callCount = 0
        return {
            executeStatement: () => {
                callCount += 1
                if (callCount <= failureCount) {
                    return Promise.reject({
                        code: "SQLITE_BUSY",
                        message: "SQLITE_BUSY: database is locked",
                    })
                }
                return Promise.resolve("ok")
            },
            getCallCount: () => {
                return callCount
            },
        }
    }

    it("should not retry a plain statement inside an open transaction", async () => {
        // Retrying it would silently commit a partial unit of work:
        // sqlite already rolled the failed statement back.
        const leaseHolder = new SqliteLeaseHolder(
            createStubRunner({
                isTransactionActive: true,
                transactionDepth: 1,
            }),
        )
        const { executeStatement, getCallCount } = createBusyFailingExecute(99)

        let hasFailed = false
        try {
            await leaseHolder.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    it("should retry COMMIT inside an open transaction", async () => {
        const leaseHolder = new SqliteLeaseHolder(
            createStubRunner({
                isTransactionActive: true,
                transactionDepth: 1,
            }),
        )
        const { executeStatement, getCallCount } = createBusyFailingExecute(2)

        await leaseHolder.run("COMMIT", executeStatement)
        expect(getCallCount()).to.equal(3)
    })

    it("should retry a statement outside a transaction until the deadline, then surface the error", async () => {
        const leaseHolder = new SqliteLeaseHolder(createStubRunner())
        const { executeStatement, getCallCount } =
            createBusyFailingExecute(9999)

        const startedAt = Date.now()
        let hasFailed = false
        try {
            await leaseHolder.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.be.greaterThan(1)
        // The 100ms budget bounds the wait in wall-clock time, not attempts.
        expect(Date.now() - startedAt).to.be.greaterThanOrEqual(100)
        expect(Date.now() - startedAt).to.be.lessThan(5000)
    })

    it("should not retry when busyErrorRetryTimeout is 0", async () => {
        const leaseHolder = new SqliteLeaseHolder(
            createStubRunner({
                driver: {
                    options: {
                        busyErrorRetryInterval: 1,
                        busyErrorRetryTimeout: 0,
                    },
                },
            }),
        )
        const { executeStatement, getCallCount } = createBusyFailingExecute(99)

        let hasFailed = false
        try {
            await leaseHolder.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })

    it("should not retry when busyErrorRetryInterval is unset", async () => {
        const leaseHolder = new SqliteLeaseHolder(
            createStubRunner({ driver: { options: {} } }),
        )
        const { executeStatement, getCallCount } = createBusyFailingExecute(99)

        let hasFailed = false
        try {
            await leaseHolder.run(
                "UPDATE thing SET name = 'x'",
                executeStatement,
            )
        } catch {
            hasFailed = true
        }
        expect(hasFailed).to.equal(true)
        expect(getCallCount()).to.equal(1)
    })
})

describe("sqlite driver > connection lease > retryable error shapes", () => {
    /**
     * Makes an executeStatement stub that fails once with the given error, then succeeds.
     */
    function createFailOnceExecute(error: unknown): {
        executeStatement: () => Promise<string>
        getCallCount: () => number
    } {
        let callCount = 0
        return {
            executeStatement: () => {
                callCount += 1
                if (callCount === 1) {
                    return Promise.reject(error)
                }
                return Promise.resolve("ok")
            },
            getCallCount: () => {
                return callCount
            },
        }
    }

    const busyShapes: [string, unknown][] = [
        ["a code property", { code: "SQLITE_BUSY" }],
        [
            "a wrapped driverError code",
            { driverError: { code: "SQLITE_BUSY_SNAPSHOT" } },
        ],
        ["only a message", new Error("SQLITE_BUSY: database is locked")],
    ]

    for (const [shape, error] of busyShapes) {
        it(`should retry a busy error carrying ${shape}`, async () => {
            const leaseHolder = new SqliteLeaseHolder(createStubRunner())
            const { executeStatement, getCallCount } =
                createFailOnceExecute(error)

            await leaseHolder.run(
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
            const leaseHolder = new SqliteLeaseHolder(createStubRunner())
            const { executeStatement, getCallCount } =
                createFailOnceExecute(error)

            let hasFailed = false
            try {
                await leaseHolder.run(
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
