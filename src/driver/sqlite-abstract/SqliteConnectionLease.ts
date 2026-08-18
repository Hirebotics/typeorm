import { TypeORMError } from "../../error"
import { IsolationLevel } from "../types/IsolationLevel"
import { BetterSqlite3QueryRunner } from "../better-sqlite3/BetterSqlite3QueryRunner"
import { SqliteQueryRunner } from "../sqlite/SqliteQueryRunner"
import { AbstractSqliteDriver } from "./AbstractSqliteDriver"
import { AbstractSqliteQueryRunner } from "./AbstractSqliteQueryRunner"

/**
 * Serialized query runners for the two sqlite drivers beacon runs.
 *
 * Upstream's sqlite drivers cache one query runner and hand the same object to every caller,
 * on the reasoning that sqlite has one connection so it can only have one query runner.
 * A query runner owns a *transaction*, though, not a connection. Sharing one lets two units of
 * work land in a single transaction: the second one's writes become a savepoint inside the
 * first one's transaction and vanish with its ROLLBACK, while its caller is told it succeeded.
 *
 * The fix is two halves, and neither works alone:
 *   1. the drivers return a fresh runner per call, so each unit of work owns its transaction
 *   2. these subclasses lease the one connection, so those transactions cannot interleave
 *
 * Shipping half 1 without half 2 is worse than the bug: a second concurrent BEGIN throws
 * SQLITE_ERROR, and the failing runner's ROLLBACK then kills the *other* runner's transaction.
 */

/** Options read off the driver. Declared in the two *ConnectionOptions.ts interfaces. */
interface SqliteLeaseOptions {
    busyErrorRetryInterval?: number
    busyErrorRetryLimit?: number
    connectionLeaseTimeout?: number
    statementCacheSize?: number
    /** better-sqlite3 */
    timeout?: number
    /** node-sqlite3 */
    busyTimeout?: number
}

const DEFAULT_RETRY_LIMIT = 10

/**
 * Both drivers apply a busy timeout we never see in `options`.
 * BetterSqlite3Driver destructures `timeout = 5000` inside createDatabaseConnection(),
 * and node-sqlite3 calls sqlite3_busy_timeout(handle, 1000) in C at open, before any option.
 * Reading the configured value alone would report 0 on an unconfigured install, which is
 * production today and the reason a "200ms" retry budget really costs 16 seconds.
 */
const IMPLICIT_BUSY_TIMEOUT = {
    betterSqlite3: 5000,
    sqlite: 1000,
}

/** Lease acquire never waits less than this, however small the retry budget works out to be. */
const MIN_LEASE_TIMEOUT = 30000

/**
 * The lease timeout has to cover a queue, not just one holder ahead of us.
 * This is the number of runners we size for; beyond it, contention surfaces as a lease timeout.
 */
const EXPECTED_CONTENDING_RUNNERS = 3

/** Wait longer than this for the connection and we log, so field stalls are visible. */
const SLOW_ACQUIRE_WARN_MS = 1000

const delay = (ms: number) => new Promise<void>((ok) => setTimeout(ok, ms))

/**
 * Retry only covers SQLITE_BUSY, so match on the code rather than the message where we can.
 * QueryFailedError copies the driver error's own enumerable props, so `code` survives the wrap.
 */
function isBusyError(err: any): boolean {
    const code = err?.code ?? err?.driverError?.code
    if (typeof code === "string" && code.startsWith("SQLITE_BUSY")) return true
    // node-sqlite3 never calls sqlite3_extended_result_codes(), and older wrappers set no code.
    return String(err?.message ?? err).includes("SQLITE_BUSY")
}

// -------------------------------------------------------------------------
// The lease
// -------------------------------------------------------------------------

type Waiter = {
    grant: () => void
    fail: (err: Error) => void
    timer: any
}

/**
 * One slot, handed out first-come-first-served.
 *
 * release() passes the slot straight to the next waiter instead of clearing `held`,
 * so a woken waiter cannot lose the slot to a runner that arrives while it is still resuming.
 */
class ConnectionLease {
    private held = false
    private waiters: Waiter[] = []

    /** SQL the current holder was running when it took the slot. Diagnostics only. */
    holderSql: string | undefined

    get queueLength(): number {
        return this.waiters.length
    }

    async acquire(sql: string, timeoutMs: number): Promise<number> {
        if (!this.held) {
            this.held = true
            this.holderSql = sql
            return 0
        }

        const blockedBy = this.holderSql
        const waitStart = Date.now()

        await new Promise<void>((resolve, reject) => {
            const waiter: Waiter = {
                grant: () => {
                    clearTimeout(waiter.timer)
                    resolve()
                },
                fail: (err) => {
                    clearTimeout(waiter.timer)
                    reject(err)
                },
                timer: undefined,
            }

            waiter.timer = setTimeout(() => {
                const index = this.waiters.indexOf(waiter)
                if (index >= 0) this.waiters.splice(index, 1)
                reject(
                    new TypeORMError(
                        `Timed out after ${timeoutMs}ms waiting for the sqlite connection. ` +
                            `Waiting on: ${sql}. Holder was running: ${
                                blockedBy ?? "unknown"
                            }.`,
                    ),
                )
            }, timeoutMs)

            // Never hold the process open just to time out a wait.
            if (typeof waiter.timer?.unref === "function") waiter.timer.unref()

            this.waiters.push(waiter)
        })

        this.holderSql = sql
        return Date.now() - waitStart
    }

    release(): void {
        const next = this.waiters.shift()
        if (next) {
            // Slot stays held; the waiter owns it the moment it resumes.
            next.grant()
        } else {
            this.held = false
            this.holderSql = undefined
        }
    }
}

/**
 * Keyed on the driver rather than the connection handle: the driver exists before the handle
 * does, and both live exactly as long as the DataSource.
 */
const leases = new WeakMap<AbstractSqliteDriver, ConnectionLease>()

function leaseFor(driver: AbstractSqliteDriver): ConnectionLease {
    let lease = leases.get(driver)
    if (!lease) {
        lease = new ConnectionLease()
        leases.set(driver, lease)
    }
    return lease
}

// -------------------------------------------------------------------------
// Slot held by one query runner
// -------------------------------------------------------------------------

/**
 * What the slot needs from the runner holding it.
 *
 * Extends the runner class so that `depth` can widen the protected transactionDepth;
 * only subclasses of AbstractSqliteQueryRunner can satisfy an interface shaped like this,
 * which is exactly the constraint we want.
 */
export interface SlotHost extends AbstractSqliteQueryRunner {
    /** transactionDepth, widened. The retry rule turns on it and release() has to reset it. */
    depth: number
}

/**
 * BEGIN IMMEDIATE takes the write lock up front.
 *
 * PowerSync writes over its own connection, so we cannot serialize it. A deferred BEGIN pins a
 * read snapshot that then fails SQLITE_BUSY_SNAPSHOT on upgrade, which retry can never clear.
 * Failing at BEGIN instead turns "unit of work half done" into "unit of work not started".
 */
export function toImmediateBegin(query: string): string {
    return query === "BEGIN TRANSACTION" ? "BEGIN IMMEDIATE" : query
}

/**
 * One runner's claim on the single connection, plus the SQLITE_BUSY retry it runs under.
 *
 * Composed rather than mixed in: a mixin cannot be used here because typeorm emits
 * declarations, and TypeScript will not emit a .d.ts for an anonymous class that has
 * protected members (TS4094).
 */
export class ConnectionSlot {
    /** Statements running right now on this runner. TypeORM issues them concurrently. */
    private inFlight = 0

    /** True once this runner owns the connection slot. */
    private holds = false

    /**
     * In-flight acquire, shared by every statement on this runner.
     *
     * Without it, two statements issued together (SubjectDatabaseEntityLoader does a
     * Promise.all before the transaction opens) would each queue for the same slot, and the
     * second would wait out the timeout for a slot its own runner already holds.
     */
    private acquisition: Promise<void> | undefined

    constructor(private readonly host: SlotHost) {}

    private get options(): SqliteLeaseOptions {
        return this.host.driver.options as SqliteLeaseOptions
    }

    /**
     * Busy timeout actually in force, including the default the driver applies for us.
     * On better-sqlite3 this is time spent blocked inside C, not awaited.
     */
    effectiveBusyTimeout(): number {
        const options = this.options
        if (options.timeout !== undefined) return options.timeout
        if (options.busyTimeout !== undefined) return options.busyTimeout
        return this.host.driver.options.type === "better-sqlite3"
            ? IMPLICIT_BUSY_TIMEOUT.betterSqlite3
            : IMPLICIT_BUSY_TIMEOUT.sqlite
    }

    private retryPolicy(): { interval: number; limit: number } {
        const options = this.options
        return {
            interval: options.busyErrorRetryInterval ?? 0,
            limit: options.busyErrorRetryLimit ?? DEFAULT_RETRY_LIMIT,
        }
    }

    /**
     * The slot is held across a whole transaction, retries included, so this wait has to
     * dominate the retry budget. Otherwise a waiter gives up on a holder that is still making
     * progress. Sized for EXPECTED_CONTENDING_RUNNERS, not for a single holder.
     */
    leaseTimeout(): number {
        const configured = this.options.connectionLeaseTimeout
        if (configured !== undefined) return configured

        const { interval, limit } = this.retryPolicy()
        const worstCaseHold = limit * (this.effectiveBusyTimeout() + interval)
        return Math.max(
            MIN_LEASE_TIMEOUT,
            worstCaseHold * EXPECTED_CONTENDING_RUNNERS,
        )
    }

    /**
     * Takes the connection slot for this runner, once.
     * Re-entrant by design: nested and concurrent statements share the one acquisition.
     */
    private acquire(sql: string): Promise<void> {
        if (!this.acquisition) {
            this.acquisition = leaseFor(this.host.driver)
                .acquire(sql, this.leaseTimeout())
                .then((waitedMs) => {
                    this.holds = true
                    if (waitedMs >= SLOW_ACQUIRE_WARN_MS) {
                        this.host.connection.logger.log(
                            "warn",
                            `Waited ${waitedMs}ms for the sqlite connection before running: ${sql}`,
                            this.host,
                        )
                    }
                })
                .catch((err) => {
                    // Let the next statement try again rather than caching the failure.
                    this.acquisition = undefined
                    throw err
                })
        }
        return this.acquisition
    }

    /**
     * Frees the slot once nothing on this runner still needs it.
     *
     * Do not simplify this to `depth === 0`. startTransaction() sets isTransactionActive
     * *before* issuing BEGIN and increments transactionDepth *after*, so a depth-only test
     * frees the slot in the middle of opening a transaction.
     */
    releaseIfIdle(): void {
        if (!this.holds) return
        if (
            this.inFlight > 0 ||
            this.host.isTransactionActive ||
            this.host.depth > 0
        )
            return
        this.forceRelease()
    }

    forceRelease(): void {
        if (!this.holds) return
        this.holds = false
        this.acquisition = undefined
        leaseFor(this.host.driver).release()
    }

    /**
     * Retry is safe only where a failed statement leaves nothing behind.
     *
     * Inside a transaction it is not: sqlite has already rolled the statement back and the unit
     * of work is incomplete, so retrying the one statement would commit a partial result.
     * COMMIT and ROLLBACK are the exceptions. Both legitimately return SQLITE_BUSY with a reader
     * present, and both must land, or the connection stays inside a transaction and the next
     * runner's BEGIN fails with SQLITE_ERROR, which no retry covers.
     *
     * Gated on transaction state rather than on the error code, because node-sqlite3 never calls
     * sqlite3_extended_result_codes(): SQLITE_BUSY_SNAPSHOT is invisible on that driver, so a
     * code-prefix rule could not be shared by both.
     */
    private isRetryable(sql: string): boolean {
        return this.host.depth === 0 || /^\s*(COMMIT|END|ROLLBACK)\b/i.test(sql)
    }

    /**
     * Runs one statement holding the slot, retrying it while sqlite reports the database busy.
     */
    async run<T>(sql: string, exec: () => Promise<T>): Promise<T> {
        this.inFlight += 1
        try {
            await this.acquire(sql)

            const { interval, limit } = this.retryPolicy()
            if (interval <= 0) return await exec()

            for (let attempt = 0; ; attempt++) {
                try {
                    return await exec()
                } catch (err) {
                    if (
                        attempt >= limit ||
                        !isBusyError(err) ||
                        !this.isRetryable(sql)
                    )
                        throw err

                    this.host.connection.logger.log(
                        "warn",
                        `SQLITE_BUSY, retrying in ${interval}ms (attempt ${
                            attempt + 1
                        } of ${limit}): ${sql}`,
                        this.host,
                    )
                    await delay(interval)
                }
            }
        } finally {
            this.inFlight -= 1
            this.releaseIfIdle()
        }
    }

    /**
     * Frees the slot unconditionally, rolling back an abandoned transaction first.
     *
     * A divergence from every other driver, which only flips isReleased. Needed because sqlite
     * has one connection: a transaction left open here would make the next runner's BEGIN fail
     * with SQLITE_ERROR rather than SQLITE_BUSY, so retry could not recover.
     *
     * Never throws. TypeORM calls release() from finally blocks throughout, so throwing would
     * both leak the slot and mask the error that got us here.
     */
    async releaseRunner(
        rollback: () => Promise<any>,
        superRelease: () => Promise<void>,
    ): Promise<void> {
        try {
            if (this.host.isTransactionActive) {
                // Only worth attempting if we hold the slot. If we do not, the BEGIN never
                // took, so there is no transaction on the connection to roll back.
                if (this.holds) {
                    try {
                        await rollback()
                    } catch {
                        // Ignored: teardown must not throw.
                    }
                }
                this.host.isTransactionActive = false
                this.host.depth = 0
                this.host.connection.logger.log(
                    "warn",
                    `Query runner released with a transaction still open. Rolled it back.`,
                    this.host,
                )
            }
            await superRelease()
        } finally {
            this.forceRelease()
        }
    }
}

// -------------------------------------------------------------------------
// Concrete runners
// -------------------------------------------------------------------------

export class SerializedSqliteQueryRunner
    extends SqliteQueryRunner
    implements SlotHost
{
    private slot = new ConnectionSlot(this)

    /** Widened from protected so ConnectionSlot can read and reset it. */
    get depth(): number {
        return this.transactionDepth
    }
    set depth(value: number) {
        this.transactionDepth = value
    }

    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        const sql = toImmediateBegin(query)
        return this.slot.run(sql, () =>
            super.query(sql, parameters, useStructuredResult),
        )
    }

    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        try {
            await super.startTransaction(isolationLevel)
        } catch (err) {
            // A BeforeTransactionStart subscriber that queries and then throws leaves the slot
            // held with no commit or rollback coming. Its catch clears isTransactionActive
            // first, so by here the idle test is accurate.
            this.slot.releaseIfIdle()
            throw err
        }
    }

    async commitTransaction(): Promise<void> {
        try {
            await super.commitTransaction()
        } finally {
            // Has to be here, not in query(). COMMIT is issued while isTransactionActive is
            // still true and transactionDepth is still 1, so the check inside run() correctly
            // declines to free the slot, and nothing else calls back.
            this.slot.releaseIfIdle()
        }
    }

    async rollbackTransaction(): Promise<void> {
        try {
            await super.rollbackTransaction()
        } finally {
            this.slot.releaseIfIdle()
        }
    }

    async release(): Promise<void> {
        return this.slot.releaseRunner(
            () => this.query("ROLLBACK"),
            () => super.release(),
        )
    }
}

/**
 * Cache of prepared statements, shared by every runner on one connection handle.
 *
 * Upstream caches statements per runner. That was free while the driver reused a single runner
 * forever; with a runner per unit of work the cache is born empty every time and identical SQL
 * re-prepares on every query, which reads the schema each time.
 *
 * Hung off the connection handle, so a destroy()/initialize() cycle cannot hand out statements
 * compiled against a closed handle. Safe to share: .all() and .run() are synchronous, the driver
 * never opens an iterator, and the lease means only one runner is ever mid-statement.
 */
const memoizedConnections = new WeakSet<object>()

function memoizePrepare(databaseConnection: any, cacheSize: number): void {
    if (!databaseConnection || memoizedConnections.has(databaseConnection))
        return
    memoizedConnections.add(databaseConnection)

    // Upstream documents statementCacheSize 0 as "do not cache". Honour it here too, otherwise
    // this memo would quietly reinstate caching that the config asked to turn off.
    if (cacheSize <= 0) return

    const prepare = databaseConnection.prepare.bind(databaseConnection)
    const cache = new Map<string, any>()

    databaseConnection.prepare = (sql: string) => {
        let stmt = cache.get(sql)
        if (!stmt) {
            stmt = prepare(sql)
            cache.set(sql, stmt)
            // Map keeps insertion order, so deleting the first key evicts FIFO.
            while (cache.size > cacheSize) {
                cache.delete(cache.keys().next().value!)
            }
        }
        return stmt
    }
}

export class SerializedBetterSqlite3QueryRunner
    extends BetterSqlite3QueryRunner
    implements SlotHost
{
    private slot = new ConnectionSlot(this)

    /** Widened from protected so ConnectionSlot can read and reset it. */
    get depth(): number {
        return this.transactionDepth
    }
    set depth(value: number) {
        this.transactionDepth = value
    }

    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        memoizePrepare(
            this.driver.databaseConnection,
            this.driver.options.statementCacheSize ?? 100,
        )
        const sql = toImmediateBegin(query)
        return this.slot.run(sql, () =>
            super.query(sql, parameters, useStructuredResult),
        )
    }

    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        try {
            await super.startTransaction(isolationLevel)
        } catch (err) {
            // See SerializedSqliteQueryRunner.startTransaction.
            this.slot.releaseIfIdle()
            throw err
        }
    }

    async commitTransaction(): Promise<void> {
        try {
            await super.commitTransaction()
        } finally {
            this.slot.releaseIfIdle()
        }
    }

    async rollbackTransaction(): Promise<void> {
        try {
            await super.rollbackTransaction()
        } finally {
            this.slot.releaseIfIdle()
        }
    }

    async release(): Promise<void> {
        return this.slot.releaseRunner(
            () => this.query("ROLLBACK"),
            () => super.release(),
        )
    }

    /**
     * Routed through query() on purpose, so the lease and the retry cover it.
     * Upstream calls databaseConnection.pragma() directly, which skips both.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Same reason, and the worst offender: loadTables() fires three of these per table at once,
     * and better-sqlite3's pragma() is synchronous, so unleased they each block the event loop
     * for the busy timeout.
     *
     * Reimplemented rather than delegated to super: the abstract version drops the attached
     * database prefix, so delegating would silently lose attached-database support.
     */
    protected async loadPragmaRecords(
        tablePath: string,
        pragma: string,
    ): Promise<any> {
        const [database, tableName] = this.splitTablePath(tablePath)
        return this.query(
            `PRAGMA ${
                database ? `"${database}".` : ""
            }${pragma}("${tableName}")`,
        )
    }
}
