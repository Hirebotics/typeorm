import { performance } from "perf_hooks"
import { AbstractSqliteDriver } from "./AbstractSqliteDriver"
import { SqliteConnectionLease } from "./SqliteConnectionLease"
import { SqliteLeaseOptions } from "./sqlite.types"

/**
 * Serializes one sqlite query runner's statements against the driver's single connection.
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
 *   2. each runner serializes its statements against the one connection
 *
 * Shipping half 1 without half 2 is worse than the bug:
 * a second concurrent BEGIN throws SQLITE_ERROR,
 * and the failing runner's ROLLBACK then kills the other runner's transaction.
 *
 * This class holds no reference to its query runner and never changes runner state.
 * It knows only what sqlite is doing, which it learns from the statements handed to run().
 * A transaction is open once a BEGIN succeeds and closed once a COMMIT or ROLLBACK succeeds.
 * That is the same fact the runner tracks in isTransactionActive and transactionDepth,
 * read off the connection instead of off the runner,
 * which is why raw query("BEGIN") transactions need no separate bookkeeping.
 */

const DEFAULT_CONNECTION_LEASE_TIMEOUT = 60_000

const DEFAULT_BUSY_ERROR_RETRY_TIMEOUT = 5_000

const DEFAULT_BUSY_ERROR_RETRY_INTERVAL = 0

/**
 * Matches a statement that opens a transaction.
 */
const TRANSACTION_BEGIN_STATEMENT =
    /^\s*BEGIN(\s+(DEFERRED|IMMEDIATE|EXCLUSIVE))?(\s+TRANSACTION)?\s*;?\s*$/i

/**
 * Matches a statement that closes a transaction.
 * COMMIT and END are aliases.
 *
 * Anchored at both ends on purpose.
 * ROLLBACK TO SAVEPOINT does not close the transaction,
 * and treating it as an end would free the connection mid-transaction.
 */
const TRANSACTION_END_STATEMENT =
    /^\s*(COMMIT|END|ROLLBACK)(\s+TRANSACTION)?\s*;?\s*$/i

/**
 * The properties a thrown sqlite error may carry.
 * Differs by sqlite driver and by whether TypeORM has wrapped it yet.
 */
interface SqliteErrorLike {
    readonly code?: unknown
    readonly message?: unknown
    readonly driverError?: { readonly code?: unknown }
}

/**
 * Keyed on the driver rather than the connection handle.
 * The driver exists before the handle does, and both live exactly as long as the DataSource.
 */
const leases = new WeakMap<AbstractSqliteDriver, SqliteConnectionLease>()

function getLeaseForDriver(
    driver: AbstractSqliteDriver,
): SqliteConnectionLease {
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
 * This class cannot serialize a writer it does not manage.
 * BEGIN or BEGIN TRANSACTION defers acquiring the write lock until needed.
 * When another writer commits first, the upgrade to writer fails with SQLITE_BUSY_SNAPSHOT.
 * No retry can clear that state, and the unit of work is left half done.
 * Failing at BEGIN instead is plain retryable SQLITE_BUSY, with no work started.
 *
 * @see https://sqlite.org/lang_transaction.html
 */
function toImmediateBegin(query: string): string {
    if (query === "BEGIN TRANSACTION") {
        return "BEGIN IMMEDIATE"
    }
    return query
}

async function sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
        setTimeout(resolve, ms)
    })
}

export class SqliteConnectionSerializer {
    private readonly lease: SqliteConnectionLease
    private inFlightStatementCount = 0
    private hasOpenTransaction = false

    /**
     * Set while this runner holds the connection or is waiting for it.
     * Statements can run concurrently on one runner.
     * They share this one acquire,
     * so a statement never queues behind the connection its own runner already holds.
     */
    private acquirePromise: Promise<void> | undefined

    constructor(private readonly driver: AbstractSqliteDriver) {
        this.lease = getLeaseForDriver(driver)
    }

    /**
     * True while a transaction opened by this runner's statements is open in sqlite.
     */
    get isTransactionOpen(): boolean {
        return this.hasOpenTransaction
    }

    /**
     * Runs one statement under exclusive use of the connection.
     *
     * The statement may be rewritten first,
     * so executeStatement must run the sql it is handed, not the sql passed in.
     */
    async run<T>(
        query: string,
        executeStatement: (sql: string) => Promise<T>,
    ): Promise<T> {
        const sql = toImmediateBegin(query)
        // Keep the increment immediately before the try.
        // If it moves inside and a throw above it skips it, the finally still decrements.
        // The undercount then frees the connection while a statement is still running.
        this.inFlightStatementCount += 1
        try {
            if (!this.acquirePromise) {
                this.acquirePromise = this.acquireConnection(sql)
            }
            await this.acquirePromise
            this.lease.currentlyRunningSql = sql

            const result = await this.executeWithBusyRetry(
                sql,
                executeStatement,
            )

            // Only a statement that succeeded changed what is open in sqlite.
            // A failed COMMIT in particular leaves the transaction open,
            // so the connection must stay held until release() rolls it back.
            if (TRANSACTION_BEGIN_STATEMENT.test(sql)) {
                this.hasOpenTransaction = true
            } else if (TRANSACTION_END_STATEMENT.test(sql)) {
                this.hasOpenTransaction = false
            }
            return result
        } finally {
            this.inFlightStatementCount -= 1
            this.releaseIfIdle()
        }
    }

    /**
     * Rolls back a transaction still open when its query runner is released.
     *
     * Sqlite has one connection.
     * A transaction left open fails every later BEGIN on it with SQLITE_ERROR
     * rather than SQLITE_BUSY, so no retry could recover.
     * A transaction is only ever open while this runner holds the connection,
     * so this rollback cannot queue behind another runner.
     *
     * Never throws.
     * Release runs while an original error may be propagating,
     * and a throw here would mask that error and leak the connection.
     */
    async rollbackOnRelease(
        executeStatement: (sql: string) => Promise<unknown>,
    ): Promise<void> {
        if (!this.hasOpenTransaction) {
            return
        }
        try {
            await this.run("ROLLBACK", executeStatement)
            this.driver.connection.logger.log(
                "warn",
                `Query runner released with a transaction still open. Rolled it back.`,
            )
        } catch {
            // The failed ROLLBACK left the transaction marked open.
            // Sqlite's transaction state is now unknown,
            // but a connection held forever wedges every later runner on this driver.
            this.hasOpenTransaction = false
            this.releaseIfIdle()
        }
    }

    /**
     * The one place driver options are read as lease options.
     */
    private get options(): SqliteLeaseOptions {
        return this.driver.options as SqliteLeaseOptions
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

    private async acquireConnection(sql: string): Promise<void> {
        try {
            await this.lease.acquire(sql, this.getConnectionLeaseTimeout())
        } catch (err) {
            // Let the next statement try again rather than caching the failure.
            this.acquirePromise = undefined
            throw err
        }
    }

    /**
     * Frees the connection once this runner has no further use for it:
     * no statement in flight and no open transaction.
     * Safe on every path, including paths that never took the connection.
     *
     * An acquire is only ever pending while its own statement is in flight,
     * so the in-flight check also covers a connection that has not been granted yet.
     */
    private releaseIfIdle(): void {
        if (
            this.acquirePromise === undefined ||
            this.inFlightStatementCount > 0 ||
            this.hasOpenTransaction
        ) {
            return
        }
        this.acquirePromise = undefined
        this.lease.release()
    }

    /**
     * Runs one statement, retrying while sqlite reports the database busy.
     * The connection is already held.
     */
    private async executeWithBusyRetry<T>(
        sql: string,
        executeStatement: (sql: string) => Promise<T>,
    ): Promise<T> {
        const retryIntervalMs = this.getBusyErrorRetryInterval()
        const retryBudgetMs = this.getBusyErrorRetryTimeout()

        // Monotonic, so a system clock step cannot stretch or cut the budget short.
        // Set on the first busy error, not on the first attempt.
        let deadline: number | undefined
        while (true) {
            try {
                return await executeStatement(sql)
            } catch (err) {
                if (
                    retryIntervalMs <= 0 ||
                    !this.isBusyError(err) ||
                    !this.canRetryStatement(sql)
                ) {
                    throw err
                }

                const now = performance.now()
                if (deadline === undefined) {
                    deadline = now + retryBudgetMs
                }
                if (now >= deadline) {
                    throw err
                }

                const retryBudgetLeftMs = Math.round(deadline - now)
                this.driver.connection.logger.log(
                    "warn",
                    `SQLITE_BUSY, retrying in ${retryIntervalMs}ms (${retryBudgetLeftMs}ms of retry budget left): ${sql}`,
                )
                await sleep(retryIntervalMs)
            }
        }
    }

    /**
     * True when the error is SQLITE_BUSY, the only error a retry can clear.
     * Checks the shapes both drivers and the QueryFailedError wrap produce.
     */
    private isBusyError(err: SqliteErrorLike): boolean {
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
    private canRetryStatement(sql: string): boolean {
        if (!this.hasOpenTransaction) {
            return true
        }
        return TRANSACTION_END_STATEMENT.test(sql)
    }
}
