import { AbstractSqliteQueryRunner } from "./AbstractSqliteQueryRunner"

/**
 * Shared shapes for the sqlite connection lease.
 * Hirebotics file, not part of upstream TypeORM.
 */

/**
 * Options for the lease and the busy retry.
 */
export interface SqliteLeaseOptions {
    /**
     * Milliseconds to wait before retrying a statement that failed with SQLITE_BUSY.
     * Sqlite allows one writer at a time, so concurrent writes surface as SQLITE_BUSY.
     *
     * Default: 0, meaning no retries.
     */
    readonly busyErrorRetryInterval?: number
    /**
     * Milliseconds one statement may spend retrying after SQLITE_BUSY.
     * The clock starts at the statement's first SQLITE_BUSY.
     * 0 means the statement fails on its first SQLITE_BUSY.
     * Only has an effect when busyErrorRetryInterval is set.
     *
     * Default: 5,000.
     */
    readonly busyErrorRetryTimeout?: number
    /**
     * Milliseconds a query runner waits for exclusive use of the connection before failing.
     * Must exceed the worst-case duration of one transaction, including busy retries.
     *
     * Default: 60,000.
     */
    readonly connectionLeaseTimeout?: number
}

/**
 * A sqlite query runner with `transactionDepth` widened from protected to public.
 *
 * The lease machinery lives outside the runner class hierarchy
 * and needs the transaction depth to determine whether a transaction is active.
 */
export type SqliteLeasedQueryRunner = AbstractSqliteQueryRunner & {
    transactionDepth: number
}

/**
 * One queued request for the connection lease.
 */
export interface SqliteLeaseWaiter {
    /**
     * Grants the lease to this waiter to continue its acquire call.
     */
    grant: () => void
    /**
     * Timeout that removes this waiter from the queue and rejects its acquire call.
     */
    timer?: NodeJS.Timeout
}

/**
 * The properties a thrown sqlite error may carry.
 * Differs by sqlite driver.
 */
export interface SqliteErrorLike {
    readonly code?: unknown
    readonly message?: unknown
    readonly driverError?: { readonly code?: unknown }
}
