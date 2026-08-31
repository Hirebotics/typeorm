import { TypeORMError } from "../../error"
import { AbstractSqliteDriver } from "./AbstractSqliteDriver"
import {
    SqliteErrorLike,
    SqliteLeasedQueryRunner,
    SqliteLeaseOptions,
    SqliteLeaseWaiter,
} from "./sqlite.types"

/**
 * Serializes sqlite query runners against the driver's single connection.
 *
 * Upstream's sqlite drivers cache one query runner and hand the same object to every
 * caller, but a query runner owns a *transaction*, not a connection. Sharing one lets
 * two units of work land in a single transaction: the second one's writes become a
 * savepoint inside the first one's transaction and vanish with its ROLLBACK, while its
 * caller is told it succeeded.
 *
 * The fix is two halves, and neither works alone:
 *   1. the drivers return a fresh runner per call, so each unit of work owns its transaction
 *   2. each runner leases the one connection, so those transactions cannot interleave
 *
 * Shipping half 1 without half 2 is worse than the bug: a second concurrent BEGIN throws
 * SQLITE_ERROR, and the failing runner's ROLLBACK then kills the other runner's transaction.
 */

const DEFAULT_BUSY_RETRY_TIMEOUT = 5000

const DEFAULT_LEASE_TIMEOUT = 60_000

async function sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
        setTimeout(resolve, ms)
    })
}

/**
 * Checks various shapes of error for SQLITE_BUSY.
 */
export function isBusyError(err: SqliteErrorLike): boolean {
    const code = err?.code ?? err?.driverError?.code
    if (typeof code === "string" && code.startsWith("SQLITE_BUSY")) {
        return true
    }
    if (String(err?.message ?? err).includes("SQLITE_BUSY")) {
        return true
    }
    return false
}

/**
 * Exclusive use of the single sqlite connection, granted first-come-first-served.
 */
export class SqliteConnectionLease {
    private isHeld = false
    private waiters: SqliteLeaseWaiter[] = []

    /** SQL the current holder is running. Diagnostics only. */
    currentlyRunningSql: string | undefined

    get queueLength(): number {
        return this.waiters.length
    }

    async acquire(sql: string, timeoutMs: number): Promise<void> {
        if (!this.isHeld) {
            this.isHeld = true
            this.currentlyRunningSql = sql
            return
        }

        const blockingSql = this.currentlyRunningSql

        await new Promise<void>((resolve, reject) => {
            const waiter: SqliteLeaseWaiter = {
                grant: () => {
                    clearTimeout(waiter.timer)
                    resolve()
                },
            }

            waiter.timer = setTimeout(() => {
                const index = this.waiters.indexOf(waiter)
                if (index >= 0) {
                    this.waiters.splice(index, 1)
                }
                reject(
                    new TypeORMError(
                        `Timed out after ${timeoutMs}ms waiting for the sqlite connection. Waiting to run: ${sql}. Blocked by: ${blockingSql}.`,
                    ),
                )
            }, timeoutMs)

            // Never hold the process open just to time out a wait.
            waiter.timer.unref()

            this.waiters.push(waiter)
        })

        this.currentlyRunningSql = sql
    }

    release(): void {
        const next = this.waiters.shift()
        if (next) {
            next.grant()
        } else {
            this.isHeld = false
            this.currentlyRunningSql = undefined
        }
    }
}

/**
 * Keyed on the driver rather than the connection handle.
 * The driver exists before the handle does, and both live exactly as long as the DataSource.
 */
const leases = new WeakMap<AbstractSqliteDriver, SqliteConnectionLease>()

function getLease(driver: AbstractSqliteDriver): SqliteConnectionLease {
    let lease = leases.get(driver)
    if (!lease) {
        lease = new SqliteConnectionLease()
        leases.set(driver, lease)
    }
    return lease
}

/**
 * BEGIN IMMEDIATE takes the write lock up front.
 *
 * Another connection can write the same database (for example a sync engine running
 * in a worker thread), and the lease cannot serialize a writer it does not manage.
 * A deferred BEGIN pins a read snapshot that later fails with SQLITE_BUSY_SNAPSHOT
 * on upgrade, which no retry can clear. Failing at BEGIN instead turns "unit of work
 * half done" into "unit of work not started", as retryable SQLITE_BUSY.
 */
export function toImmediateBegin(query: string): string {
    if (query === "BEGIN TRANSACTION") {
        return "BEGIN IMMEDIATE"
    }
    return query
}

/**
 * One runner's lease on the single connection, plus the SQLITE_BUSY retry it runs under.
 *
 * Composed rather than mixed into the runner classes: typeorm emits declaration files,
 * and TypeScript will not emit a .d.ts for an anonymous class with protected members (TS4094).
 */
export class SqliteLeaseHolder {
    private readonly lease: SqliteConnectionLease
    private inFlightStatementCount = 0
    private hasLease = false

    /**
     * True while a transaction opened by a raw query("BEGIN ...") is open in sqlite.
     * A raw BEGIN sets no runner flag, and the lease must stay held until that
     * transaction ends too.
     */
    private isRawTransactionOpen = false

    /**
     * Statements can run concurrently on one runner. They share one in-flight
     * acquire, so a statement never queues behind the lease its own runner
     * already holds.
     */
    private acquirePromise: Promise<void> | undefined

    constructor(private readonly runner: SqliteLeasedQueryRunner) {
        this.lease = getLease(runner.driver)
    }

    private get options(): SqliteLeaseOptions {
        return this.runner.driver.options as SqliteLeaseOptions
    }

    private getLeaseTimeoutMs(): number {
        return this.options.connectionLeaseTimeout ?? DEFAULT_LEASE_TIMEOUT
    }

    private acquire(sql: string): Promise<void> {
        if (!this.acquirePromise) {
            this.acquirePromise = this.acquireLease(sql)
        }
        return this.acquirePromise
    }

    private async acquireLease(sql: string): Promise<void> {
        try {
            await this.lease.acquire(sql, this.getLeaseTimeoutMs())
            this.hasLease = true
        } catch (err) {
            // Let the next statement try again rather than caching the failure.
            this.acquirePromise = undefined
            throw err
        }
    }

    /**
     * Frees the lease once nothing on this runner still needs it: no statement in
     * flight, no managed transaction, no raw transaction. A no-op when the lease is
     * not held, so it is safe to call on every path.
     *
     * Do not simplify the checks to `transactionDepth === 0`: while a transaction
     * opens, isTransactionActive becomes true before transactionDepth increments,
     * so both must be checked.
     */
    releaseIfIdle(): void {
        if (!this.hasLease) {
            return
        }
        if (
            this.inFlightStatementCount > 0 ||
            this.runner.isTransactionActive ||
            this.runner.transactionDepth > 0 ||
            this.isRawTransactionOpen
        ) {
            return
        }
        this.forceRelease()
    }

    /**
     * Recovers from a BEGIN that failed before taking effect. At depth 0 nothing is
     * open in sqlite, but isTransactionActive may already be true. Reset it so the
     * lease can be freed instead of staying held until the runner is released.
     */
    releaseAfterFailedBegin(): void {
        if (this.runner.transactionDepth === 0) {
            this.runner.isTransactionActive = false
        }
        this.releaseIfIdle()
    }

    forceRelease(): void {
        if (!this.hasLease) {
            return
        }
        this.hasLease = false
        this.acquirePromise = undefined
        this.lease.release()
    }

    /**
     * Retry is safe only where a failed statement leaves nothing behind. Inside a
     * transaction it is not: sqlite has already rolled the failed statement back, so
     * retrying it alone would commit a partial unit of work. COMMIT and ROLLBACK are
     * the exceptions -- both legitimately return SQLITE_BUSY while a reader is present,
     * and both must land, or the transaction left open fails every later BEGIN on the
     * connection with non-retryable SQLITE_ERROR.
     *
     * The gate is transaction state rather than the error code because node-sqlite3
     * never calls sqlite3_extended_result_codes(): SQLITE_BUSY_SNAPSHOT is invisible
     * there, so a code-prefix rule cannot be shared by both drivers.
     */
    private isRetryable(sql: string): boolean {
        if (this.runner.transactionDepth === 0) {
            return true
        }
        return /^\s*(COMMIT|END|ROLLBACK)\b/i.test(sql)
    }

    /**
     * Tracks transactions opened and closed by raw transaction-control statements.
     * A BEGIN or COMMIT that arrives while the runner flags already track a
     * transaction is managed transaction control, not raw, and is ignored here.
     */
    private noteRawTransactionControl(sql: string): void {
        if (
            this.runner.isTransactionActive ||
            this.runner.transactionDepth > 0
        ) {
            return
        }
        if (/^\s*BEGIN\b/i.test(sql)) {
            this.isRawTransactionOpen = true
            return
        }
        if (/^\s*(COMMIT|END|ROLLBACK)\b/i.test(sql)) {
            this.isRawTransactionOpen = false
        }
    }

    /**
     * Runs one statement under the lease, retrying while sqlite reports the database busy.
     */
    async run<T>(sql: string, executeStatement: () => Promise<T>): Promise<T> {
        this.inFlightStatementCount += 1
        try {
            await this.acquire(sql)
            this.lease.currentlyRunningSql = sql

            const retryIntervalMs = this.options.busyErrorRetryInterval ?? 0
            const retryBudgetMs =
                this.options.busyErrorRetryTimeout ?? DEFAULT_BUSY_RETRY_TIMEOUT

            let deadline: number | undefined
            for (;;) {
                try {
                    const result = await executeStatement()
                    this.noteRawTransactionControl(sql)
                    return result
                } catch (err) {
                    if (
                        retryIntervalMs <= 0 ||
                        !isBusyError(err) ||
                        !this.isRetryable(sql)
                    ) {
                        throw err
                    }

                    const now = Date.now()
                    if (deadline === undefined) {
                        deadline = now + retryBudgetMs
                    }
                    if (now >= deadline) {
                        throw err
                    }

                    this.runner.connection.logger.log(
                        "warn",
                        `SQLITE_BUSY, retrying in ${retryIntervalMs}ms (${
                            deadline - now
                        }ms of retry budget left): ${sql}`,
                        this.runner,
                    )
                    await sleep(retryIntervalMs)
                }
            }
        } finally {
            this.inFlightStatementCount -= 1
            this.releaseIfIdle()
        }
    }

    /**
     * Releases the runner, rolling back an abandoned transaction first.
     *
     * A divergence from every other driver, which only flips isReleased here. Sqlite
     * has one connection: a transaction left open would fail every later BEGIN on it
     * with SQLITE_ERROR rather than SQLITE_BUSY, so retry could not recover.
     *
     * The abandoned-transaction gate checks transactionDepth and the raw flag as
     * well as isTransactionActive: a nested BEGIN that fails can leave
     * isTransactionActive false while the outer transaction is still open in sqlite,
     * and a raw BEGIN sets no runner flag at all.
     *
     * Never throws: release runs while an original error may be propagating, and a
     * throw here would mask that error and leak the lease.
     */
    async releaseRunner(superRelease: () => Promise<void>): Promise<void> {
        if (this.runner.isReleased) {
            return
        }
        try {
            if (
                this.runner.isTransactionActive ||
                this.runner.transactionDepth > 0 ||
                this.isRawTransactionOpen
            ) {
                // Without the lease the BEGIN never took effect, so there is nothing
                // open in sqlite to roll back.
                if (this.hasLease) {
                    try {
                        await this.runner.query("ROLLBACK")
                        this.runner.connection.logger.log(
                            "warn",
                            `Query runner released with a transaction still open. Rolled it back.`,
                            this.runner,
                        )
                    } catch {
                        // Ignored: teardown must not throw.
                    }
                }
                this.runner.isTransactionActive = false
                this.runner.transactionDepth = 0
                this.isRawTransactionOpen = false
            }
            this.runner.isReleased = true
            await superRelease()
        } finally {
            // releaseIfIdle rather than forceRelease: releasing while a statement is
            // still in flight must leave the lease to that statement, or the next
            // runner starts while the old statement is still executing.
            this.releaseIfIdle()
        }
    }
}
