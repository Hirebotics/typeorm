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
 * Hirebotics file, not part of upstream TypeORM.
 *
 * Upstream's sqlite drivers cache one query runner and hand the same object to every caller.
 * A query runner owns a *transaction*, not a connection.
 * Sharing one lets two units of work land in a single transaction:
 * the second one's writes become a savepoint inside the first one's transaction,
 * they vanish with its ROLLBACK, and the second caller is told it succeeded.
 *
 * The fix is two halves, and neither works alone:
 *   1. the drivers return a fresh runner per call, so each unit of work owns its transaction
 *   2. each runner leases the one connection, so those transactions cannot interleave
 *
 * Shipping half 1 without half 2 is worse than the bug:
 * a second concurrent BEGIN throws SQLITE_ERROR,
 * and the failing runner's ROLLBACK then kills the other runner's transaction.
 */

const DEFAULT_CONNECTION_LEASE_TIMEOUT = 60_000

const DEFAULT_BUSY_ERROR_RETRY_TIMEOUT = 5_000

const DEFAULT_BUSY_ERROR_RETRY_INTERVAL = 0

/**
 * Matches statements that open a transaction.
 */
const TRANSACTION_BEGIN_STATEMENT = /^\s*BEGIN\b/i

/**
 * Matches statements that close a transaction.
 * COMMIT and END are aliases.
 */
const TRANSACTION_END_STATEMENT = /^\s*(COMMIT|END|ROLLBACK)\b/i

async function sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
        setTimeout(resolve, ms)
    })
}

/**
 * Exclusive use of the single sqlite connection, granted first-come-first-served.
 */
export class SqliteConnectionLease {
    private isHeld = false
    private waiters: SqliteLeaseWaiter[] = []

    /**
     * SQL the current holder is running. Diagnostics only.
     */
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
 * Another connection can write the same database, such as a sync engine in a worker thread.
 * The lease cannot serialize a writer it does not manage.
 * BEGIN or BEGIN TRANSACTION defers acquiring the write lock until needed.
 * When another writer commits first, the upgrade to writer fails with SQLITE_BUSY_SNAPSHOT.
 * No retry can clear that state, and the unit of work is left half done.
 * Failing at BEGIN instead is plain retryable SQLITE_BUSY, with no work started.
 *
 * @see https://sqlite.org/lang_transaction.html
 */
export function toImmediateBegin(query: string): string {
    if (query === "BEGIN TRANSACTION") {
        return "BEGIN IMMEDIATE"
    }
    return query
}

/**
 * One query runner's lease on the single connection, plus the SQLITE_BUSY retry it runs under.
 */
export class SqliteLeaseHolder {
    private readonly lease: SqliteConnectionLease
    private inFlightStatementCount = 0
    private hasLease = false

    /**
     * True while a transaction opened by a raw query("BEGIN ...") is open in sqlite.
     * Query runners only track transactions opened through their startTransaction().
     * But queries that execute a raw BEGIN or COMMIT can open a transaction too,
     * so raw transactions need this field to keep the lease held until they end.
     */
    private isRawTransactionOpen = false

    /**
     * Statements can run concurrently on one runner.
     * They share one in-flight acquire,
     * so a statement never queues behind the lease its own runner already holds.
     */
    private acquirePromise: Promise<void> | undefined

    constructor(private readonly runner: SqliteLeasedQueryRunner) {
        this.lease = getLease(runner.driver)
    }

    private get options(): SqliteLeaseOptions {
        return this.runner.driver.options as SqliteLeaseOptions
    }

    private getConnectionLeaseTimeout(): number {
        return (
            this.options.connectionLeaseTimeout ??
            DEFAULT_CONNECTION_LEASE_TIMEOUT
        )
    }

    private getBusyErrorRetryTimeout(): number {
        return (
            this.options.busyErrorRetryTimeout ??
            DEFAULT_BUSY_ERROR_RETRY_TIMEOUT
        )
    }

    private getBusyErrorRetryInterval(): number {
        return (
            this.options.busyErrorRetryInterval ??
            DEFAULT_BUSY_ERROR_RETRY_INTERVAL
        )
    }

    private async acquireLease(sql: string): Promise<void> {
        try {
            await this.lease.acquire(sql, this.getConnectionLeaseTimeout())
            this.hasLease = true
        } catch (err) {
            // Let the next statement try again rather than caching the failure.
            this.acquirePromise = undefined
            throw err
        }
    }

    /**
     * True while a transaction opened through startTransaction() is open in sqlite.
     *
     * Do not simplify the checks to `transactionDepth === 0`.
     * While a transaction opens, isTransactionActive becomes true before
     * transactionDepth increments, so both must be checked.
     */
    private isManagedTransactionOpen(): boolean {
        return (
            this.runner.isTransactionActive || this.runner.transactionDepth > 0
        )
    }

    /**
     * True while any transaction on this runner, managed or raw, is open in sqlite.
     */
    private isTransactionOpen(): boolean {
        return this.isManagedTransactionOpen() || this.isRawTransactionOpen
    }

    /**
     * Frees the lease once nothing on this runner still needs it:
     * no statement in flight and no open transaction.
     * Safe to call on every path: a no-op when the lease is not held.
     */
    releaseIfIdle(): void {
        if (
            !this.hasLease ||
            this.inFlightStatementCount > 0 ||
            this.isTransactionOpen()
        ) {
            return
        }
        this.hasLease = false
        this.acquirePromise = undefined
        this.lease.release()
    }

    /**
     * Recovers from a BEGIN that failed before taking effect.
     * At depth 0 nothing is open in sqlite, but isTransactionActive may already be true.
     * Reset it so the lease frees now instead of staying held until the runner is released.
     */
    releaseAfterFailedBegin(): void {
        if (this.runner.transactionDepth === 0) {
            this.runner.isTransactionActive = false
        }
        this.releaseIfIdle()
    }

    /**
     * True when the error is SQLITE_BUSY, the only error retry can clear.
     * Checks the shapes both drivers and the QueryFailedError wrap produce.
     */
    private isRetryableError(err: SqliteErrorLike): boolean {
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
     * Retry is safe only where a failed statement leaves nothing behind.
     *
     * Inside a transaction it is not:
     * sqlite has already rolled the failed statement back,
     * so retrying it alone would commit a partial unit of work.
     *
     * COMMIT and ROLLBACK are the exceptions.
     * Both legitimately return SQLITE_BUSY while a reader is present, and both must land,
     * or the transaction left open fails every later BEGIN with non-retryable SQLITE_ERROR.
     */
    private isRetryableStatement(sql: string): boolean {
        if (this.runner.transactionDepth === 0) {
            return true
        }
        return TRANSACTION_END_STATEMENT.test(sql)
    }

    /**
     * Tracks transactions opened and closed by raw transaction-control statements.
     */
    private trackRawTransactionControl(sql: string): void {
        // The query runner already knows a transaction is open, nothing to do.
        if (this.isManagedTransactionOpen()) {
            return
        }
        // Transaction opened by a raw BEGIN query.
        if (TRANSACTION_BEGIN_STATEMENT.test(sql)) {
            this.isRawTransactionOpen = true
            return
        }
        // Transaction closed by a raw COMMIT, END, or ROLLBACK query.
        if (TRANSACTION_END_STATEMENT.test(sql)) {
            this.isRawTransactionOpen = false
        }
    }

    /**
     * Runs one statement under the lease, retrying while sqlite reports the database busy.
     */
    async run<T>(sql: string, executeStatement: () => Promise<T>): Promise<T> {
        // Keep the increment immediately before the try.
        // If it moves inside and a throw above it skips it, the finally still decrements.
        // The undercount then frees the lease while a statement is still running.
        this.inFlightStatementCount += 1
        try {
            if (!this.acquirePromise) {
                this.acquirePromise = this.acquireLease(sql)
            }
            await this.acquirePromise
            this.lease.currentlyRunningSql = sql

            const retryIntervalMs = this.getBusyErrorRetryInterval()
            const retryBudgetMs = this.getBusyErrorRetryTimeout()

            let deadline: number | undefined
            while (true) {
                try {
                    const result = await executeStatement()
                    this.trackRawTransactionControl(sql)
                    return result
                } catch (err) {
                    if (
                        retryIntervalMs <= 0 ||
                        !this.isRetryableError(err) ||
                        !this.isRetryableStatement(sql)
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

                    const retryBudgetLeftMs = Math.round(deadline - now)
                    this.runner.connection.logger.log(
                        "warn",
                        `SQLITE_BUSY, retrying in ${retryIntervalMs}ms (${retryBudgetLeftMs}ms of retry budget left): ${sql}`,
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
     * A divergence from every other driver, which only flips isReleased here.
     * Sqlite has one connection.
     * A transaction left open would fail every later BEGIN on it with SQLITE_ERROR
     * rather than SQLITE_BUSY, so retry could not recover.
     *
     * Never throws.
     * Release runs while an original error may be propagating,
     * and a throw here would mask that error and leak the lease.
     *
     * onRelease is the runner's own release work.
     * It runs at most once, after teardown, and must not throw.
     * It stays out of the finally block on purpose:
     * the finally must contain only the lease-freeing,
     * or a throwing callback would leak the lease permanently.
     */
    async releaseRunner(onRelease: () => Promise<void>): Promise<void> {
        if (this.runner.isReleased) {
            return
        }
        try {
            if (this.isTransactionOpen()) {
                // Without the lease the BEGIN never took effect,
                // so there is nothing open in sqlite to roll back.
                // An unconditional ROLLBACK would also queue for the lease,
                // and release() must never block behind other runners.
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
            await onRelease()
        } finally {
            // releaseIfIdle rather than an unconditional release:
            // a statement still in flight must keep the lease until its own finally runs,
            // or the next runner starts while the old statement is still executing.
            this.releaseIfIdle()
        }
    }
}
