import { QueryRunnerAlreadyReleasedError, TypeORMError } from "../../error"
import { Logger } from "../../logger/Logger"

/**
 * Exclusive leasing of a sqlite driver's single connection.
 * Hirebotics file, not part of upstream TypeORM.
 *
 * Upstream cached one query runner and gave every caller the same object.
 * A query runner owns a transaction, not a connection.
 * So two concurrent units of work landed in one transaction.
 * The second became a SAVEPOINT inside the first, and died with the first's
 * ROLLBACK while its caller was told it committed.
 *
 * The connection stays single because each connection to ':memory:' is a
 * separate database, and consumers run ':memory:' in tests.
 * That is what makes leasing necessary, not any limit in sqlite itself.
 *
 * A runner leases the connection for its whole life and gives it back on
 * release, which revokes the lease for good.
 * A statement that arrives after that throws instead of running inside the
 * next runner's transaction, where the rollback would discard the write.
 *
 * Everything deciding who may touch the connection lives here.
 * A query runner holds a lease and nothing else.
 */

const DEFAULT_ACQUIRE_TIMEOUT_MS = 60_000

/**
 * Typed loosely to match the driver's own connection field.
 */
type SqliteConnection = any

export interface SqliteConnectionPoolOptions {
    /**
     * Reads the driver's connection.
     * A function because the pool is built before the driver opens the handle.
     * Typed loosely to match the driver's own connection field.
     */
    getConnection: () => SqliteConnection

    /**
     * Rolls back whatever a holder left open, straight on the connection.
     * True when it rolled something back, false when nothing was open.
     * Rejects when neither of those could be established.
     */
    rollback: () => Promise<boolean>

    logger: Logger

    acquireTimeoutMs?: number
}

/**
 * One query runner's claim on the connection.
 * Starts in line, is granted its turn, and is revoked once and for good.
 */
export class SqliteConnectionLease {
    private connection: SqliteConnection
    private revokedReason: Error | undefined
    private releasePromise: Promise<void> | undefined
    private waitTimer: NodeJS.Timeout | undefined
    private queuedAtMs = Date.now()

    /**
     * Settles when this lease is granted or revoked, whichever comes first.
     * It never rejects, so a lease nobody waits on cannot crash the process.
     */
    private settled: Promise<void>
    private settle!: () => void

    constructor(private pool: SqliteConnectionPool) {
        this.settled = new Promise<void>((ok) => {
            this.settle = ok
        })
    }

    /**
     * Waits for this lease's turn, then returns the connection.
     */
    async getConnection(): Promise<SqliteConnection> {
        await this.settled
        this.assertNotRevoked()
        return this.connection
    }

    /**
     * Throws once this lease has been revoked.
     *
     * Nothing may be awaited between this call and the statement it guards.
     * An await after it reopens the window it closes, because release() can
     * revoke the lease in the meantime and the statement would then run
     * inside the next runner's transaction.
     */
    assertNotRevoked(): void {
        if (this.revokedReason) {
            throw this.revokedReason
        }
    }

    /**
     * Gives the connection back.
     * Safe to call any number of times, whether or not the turn ever came.
     */
    async release(): Promise<void> {
        if (!this.releasePromise) {
            this.releasePromise = this.pool.releaseLease(this)
        }
        return this.releasePromise
    }

    grant(connection: SqliteConnection): void {
        this.clearWaitTimer()
        this.connection = connection
        this.settle()
    }

    /**
     * Reclaims the connection and refuses this lease.
     * A revoked lease can never be granted a turn nobody will end.
     */
    revoke(reason: Error): void {
        if (this.revokedReason) {
            return
        }
        this.clearWaitTimer()
        this.pool.dropFromQueue(this)
        this.connection = undefined
        this.revokedReason = reason
        this.settle()
    }

    /**
     * Fails this lease if its turn never comes.
     *
     * Never unref the timer.
     * A process whose pending work is this wait would exit 0 mid-transaction,
     * with the caller's promise unsettled and its finally block never run.
     */
    startWaitTimer(timeoutMs: number): void {
        this.waitTimer = setTimeout(() => {
            this.revoke(this.buildTimeoutError())
        }, timeoutMs)
    }

    private clearWaitTimer(): void {
        if (this.waitTimer) {
            clearTimeout(this.waitTimer)
            this.waitTimer = undefined
        }
    }

    /**
     * Reports the wait that elapsed, not the deadline that was configured.
     * better-sqlite3 blocks the event loop inside sqlite3_step, so this timer
     * can fire late and a message quoting the deadline would understate the hold.
     */
    private buildTimeoutError(): TypeORMError {
        const elapsedMs = Date.now() - this.queuedAtMs
        return new TypeORMError(
            `Timed out after ${elapsedMs}ms waiting for the sqlite connection. ` +
                `A query runner was never released, or a second query runner was ` +
                `created while the first still held an open transaction.`,
        )
    }
}

export class SqliteConnectionPool {
    private heldLease: SqliteConnectionLease | undefined
    private queue: SqliteConnectionLease[] = []

    /**
     * Set once the connection can no longer be trusted or no longer exists.
     * Every lease from then on is refused with the reason.
     */
    private refusalReason: Error | undefined

    constructor(private options: SqliteConnectionPoolOptions) {}

    /**
     * Takes a place in line for the connection.
     *
     * Returns before the turn comes.
     * The caller always holds the lease it will have to give back.
     * Waiting for a lease in order to name it would deadlock.
     * The connection is freed by a release the caller cannot reach while waiting.
     */
    acquire(): SqliteConnectionLease {
        const lease = new SqliteConnectionLease(this)
        if (this.refusalReason) {
            lease.revoke(this.refusalReason)
            return lease
        }
        if (this.heldLease) {
            this.queue.push(lease)
            lease.startWaitTimer(
                this.options.acquireTimeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS,
            )
            return lease
        }
        this.grantTo(lease)
        return lease
    }

    /**
     * Gives the connection back and hands it to whoever is next in line.
     *
     * A lease waiting its turn simply leaves the queue, nothing to rollback.
     * Rolling back there would roll back the holder's transaction, and the
     * waiting turn would still be granted later with nobody left to end it.
     */
    async releaseLease(lease: SqliteConnectionLease): Promise<void> {
        const wasHeld = lease === this.heldLease

        // Revoked before the rollback, so a statement racing this release is
        // refused rather than committed in autocommit behind the rollback.
        lease.revoke(new QueryRunnerAlreadyReleasedError())

        if (!wasHeld) {
            return
        }
        try {
            await this.rollbackAbandonedTransaction()
        } finally {
            this.heldLease = undefined
            this.grantToNextInLine()
        }
    }

    /**
     * Refuses the connection from now on, and takes it off whoever holds it.
     *
     * Without this a lease outlives the handle it was granted, and a statement
     * on it reaches a closed connection instead of a clear error.
     */
    close(): void {
        this.refuse(
            new TypeORMError(
                `The sqlite connection was closed because the DataSource was destroyed.`,
            ),
        )
    }

    dropFromQueue(lease: SqliteConnectionLease): void {
        const index = this.queue.indexOf(lease)
        if (index >= 0) {
            this.queue.splice(index, 1)
        }
    }

    /**
     * Rolls back whatever the holder left open.
     *
     * Never gated on the runner's transaction flags.
     * A caller that ran a raw BEGIN never sets them,
     * and its transaction would then outlive the runner.
     *
     * There are three outcomes:
     *  1. Rolled back: warn, then hand the connection on.
     *  2. Nothing was open: hand it on, say nothing.
     *  3. Anything else: the transaction state is unknown, so refuse the connection.
     *     Handing it on would let the next runner read the abandoned rows,
     *     and its BEGIN would fail for as long as the process lives.
     */
    private async rollbackAbandonedTransaction(): Promise<void> {
        let wasRolledBack: boolean
        try {
            wasRolledBack = await this.options.rollback()
        } catch (err) {
            this.refuse(
                new TypeORMError(
                    `The sqlite connection was left with an abandoned transaction: the rollback on release failed. ${err}`,
                ),
            )
            return
        }
        if (wasRolledBack) {
            this.options.logger.log(
                "warn",
                `Query runner released with a transaction still open. Rolled it back.`,
            )
        }
    }

    private refuse(reason: Error): void {
        this.refusalReason = reason
        const refused = [...this.queue, this.heldLease]
        this.queue = []
        this.heldLease = undefined
        for (const lease of refused) {
            lease?.revoke(reason)
        }
    }

    private grantToNextInLine(): void {
        const next = this.queue.shift()
        if (next) {
            this.grantTo(next)
        }
    }

    private grantTo(lease: SqliteConnectionLease): void {
        this.heldLease = lease
        lease.grant(this.options.getConnection())
    }
}
