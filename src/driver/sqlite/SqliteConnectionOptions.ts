import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"

/**
 * Sqlite-specific connection options.
 */
export interface SqliteConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type.
     */
    readonly type: "sqlite"

    /**
     * Storage type or path to the storage.
     */
    readonly database: string

    /**
     * The driver object
     * This defaults to require("sqlite3")
     */
    readonly driver?: any

    /**
     * Encryption key for for SQLCipher.
     */
    readonly key?: string

    /**
     * In your SQLite application when you perform parallel writes its common to face SQLITE_BUSY error.
     * This error indicates that SQLite failed to write to the database file since someone else already writes into it.
     * Since SQLite cannot handle parallel saves this error cannot be avoided.
     *
     * To simplify life's of those who have this error this particular option sets a timeout within which ORM will try
     * to perform requested write operation again and again until it receives SQLITE_BUSY error.
     *
     * Enabling WAL can improve your app performance and face less SQLITE_BUSY issues.
     * Time in milliseconds.
     *
     * @deprecated Retries forever, and only on this driver.
     * Use busyErrorRetryInterval and busyErrorRetryLimit instead:
     * they are bounded, and they work the same way on better-sqlite3.
     * Setting both arms two stacked retry loops.
     */
    readonly busyErrorRetry?: number

    /**
     * Enables WAL mode. By default its disabled.
     *
     * @see https://www.sqlite.org/wal.html
     */
    readonly enableWAL?: boolean

    /**
     * Specifies the open file flags. By default its undefined.
     * @see https://www.sqlite.org/c3ref/c_open_autoproxy.html
     * @see https://github.com/TryGhost/node-sqlite3/blob/master/test/open_close.test.js
     */
    readonly flags?: number

    readonly poolSize?: never

    /**
     * Query or change the setting of the busy timeout.
     * Time in milliseconds.
     *
     * @see https://www.sqlite.org/pragma.html#pragma_busy_timeout
     */
    readonly busyTimeout?: number

    /**
     * Milliseconds to wait before retrying a statement that failed with SQLITE_BUSY.
     * Sqlite allows one writer at a time, so concurrent writes surface as SQLITE_BUSY.
     *
     * Default: 0, meaning no retries.
     */
    readonly busyErrorRetryInterval?: number

    /**
     * How many times one statement may be retried after SQLITE_BUSY. Must be above 0.
     * Only has an effect when busyErrorRetryInterval is set.
     *
     * Default: 10.
     */
    readonly busyErrorRetryLimit?: number

    /**
     * Milliseconds a query runner waits for exclusive use of the connection before failing.
     */
    readonly connectionLeaseTimeout?: number
}
