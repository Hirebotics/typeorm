/**
 * Shared sqlite shapes.
 * Hirebotics file, not part of upstream TypeORM.
 */

/**
 * Options for serializing query runners against the single sqlite connection.
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
