import { TypeORMError } from "../../error"

/**
 * Exclusive use of one sqlite driver's single connection.
 * Hirebotics file, not part of upstream TypeORM.
 *
 * Upstream cached one query runner and handed the same object to every caller.
 * A query runner owns a transaction, not a connection, so two concurrent units of work
 * landed in one transaction: the second became a SAVEPOINT inside the first and died
 * with the first's ROLLBACK while its caller was told it had committed.
 *
 * The connection stays single because each connection to ':memory:' is a separate
 * database and consumers run ':memory:' in tests. That is what makes this lock
 * necessary, not any limit in sqlite itself.
 *
 * sqlite's own busy handler cannot do this job. Two runners on one connection do not
 * contend for a file lock, and the second BEGIN fails SQLITE_ERROR rather than
 * SQLITE_BUSY. A busy wait would also block the event loop that the holder needs in
 * order to reach its COMMIT.
 */

const DEFAULT_ACQUIRE_TIMEOUT_MS = 60_000

interface SqliteLockWaiter {
    grant: () => void
    reject: (err: Error) => void
}

export class SqliteConnectionLock {
    private isHeld = false
    private waiters: SqliteLockWaiter[] = []

    constructor(private acquireTimeoutMs = DEFAULT_ACQUIRE_TIMEOUT_MS) {}

    /**
     * Grants the connection, first come first served.
     * The returned function is the only way to give it back, so a caller cannot
     * release a lock it does not hold or release the same one twice.
     */
    async acquire(): Promise<() => void> {
        if (this.isHeld) {
            await this.waitInQueue()
        }
        this.isHeld = true

        let isReleased = false
        return () => {
            if (isReleased) {
                return
            }
            isReleased = true
            this.grantToNextWaiter()
        }
    }

    /**
     * Fails everyone still queued.
     * Without this a waiter outlives the handle it queued for and is later granted
     * a closed connection.
     */
    destroy(): void {
        const queued = this.waiters
        this.waiters = []
        this.isHeld = false
        for (const waiter of queued) {
            waiter.reject(
                new TypeORMError(
                    `The DataSource was destroyed while waiting for the sqlite connection.`,
                ),
            )
        }
    }

    private async waitInQueue(): Promise<void> {
        const startedAtMs = Date.now()
        await new Promise<void>((resolve, reject) => {
            const waiter: SqliteLockWaiter = { grant: resolve, reject }
            const timer = setTimeout(() => {
                this.removeWaiter(waiter)
                reject(this.buildTimeoutError(startedAtMs))
            }, this.acquireTimeoutMs)

            waiter.grant = () => {
                clearTimeout(timer)
                resolve()
            }
            waiter.reject = (err) => {
                clearTimeout(timer)
                reject(err)
            }
            this.waiters.push(waiter)
        })
    }

    /**
     * Reports the wait that elapsed, not the deadline that was configured.
     * better-sqlite3 blocks the event loop inside sqlite3_step, so this timer can fire
     * seconds late and a message quoting the deadline would understate the hold.
     */
    private buildTimeoutError(startedAtMs: number): TypeORMError {
        const elapsedMs = Date.now() - startedAtMs
        return new TypeORMError(
            `Timed out after ${elapsedMs}ms waiting for the sqlite connection. ` +
                `A query runner was never released, or a second query runner was ` +
                `created while the first still held an open transaction.`,
        )
    }

    private removeWaiter(waiter: SqliteLockWaiter): void {
        const index = this.waiters.indexOf(waiter)
        if (index >= 0) {
            this.waiters.splice(index, 1)
        }
    }

    private grantToNextWaiter(): void {
        const next = this.waiters.shift()
        if (next) {
            next.grant()
            return
        }
        this.isHeld = false
    }
}
