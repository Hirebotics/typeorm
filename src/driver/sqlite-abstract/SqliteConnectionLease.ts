import { TypeORMError } from "../../error"

/**
 * Exclusive use of one sqlite driver's single connection, granted first-come-first-served.
 * Hirebotics file, not part of upstream TypeORM.
 *
 * Holds no opinion about transactions or statements.
 * SqliteConnectionSerializer decides when a runner takes the connection and when it gives it back.
 */

/**
 * One queued request for the connection.
 */
interface SqliteLeaseWaiter {
    /**
     * Grants the connection to this waiter, resuming its acquire call.
     */
    grant: () => void
    /**
     * Timer that drops this waiter from the queue and rejects its acquire call.
     */
    timer?: NodeJS.Timeout
}

export class SqliteConnectionLease {
    private isHeld = false
    private waiters: SqliteLeaseWaiter[] = []

    /**
     * SQL the current holder is running.
     * Names the blocking statement in the timeout message.
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
