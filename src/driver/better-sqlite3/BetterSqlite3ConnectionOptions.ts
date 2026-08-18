import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"

/**
 * Sqlite-specific connection options.
 */
export interface BetterSqlite3ConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type.
     */
    readonly type: "better-sqlite3"

    /**
     * Storage type or path to the storage.
     */
    readonly database: string

    /**
     * The driver object
     * This defaults to require("better-sqlite3")
     */
    readonly driver?: any

    /**
     * Encryption key for for SQLCipher.
     */
    readonly key?: string

    /**
     * Cache size of sqlite statement to speed up queries.
     * Default: 100.
     */
    readonly statementCacheSize?: number

    /**
     * Function to run before a database is used in typeorm.
     * You can set pragmas, register plugins or register
     * functions or aggregates in this function.
     */
    readonly prepareDatabase?: (db: any) => void | Promise<void>

    /**
     * Open the database connection in readonly mode.
     * Default: false.
     */
    readonly readonly?: boolean

    /**
     * If the database does not exist, an Error will be thrown instead of creating a new file.
     * This option does not affect in-memory or readonly database connections.
     * Default: false.
     */
    readonly fileMustExist?: boolean

    /**
     * The number of milliseconds to wait when executing queries
     * on a locked database, before throwing a SQLITE_BUSY error.
     * Default: 5000.
     */
    readonly timeout?: number

    /**
     * Provide a function that gets called with every SQL string executed by the database connection.
     */
    readonly verbose?: Function

    /**
     * Relative or absolute path to the native addon (better_sqlite3.node).
     */
    readonly nativeBinding?: string

    readonly poolSize?: never

    /**
     * Enables WAL mode. By default its disabled.
     *
     * @see https://www.sqlite.org/wal.html
     */
    readonly enableWAL?: boolean

    /**
     * Milliseconds to wait before retrying a statement that failed with SQLITE_BUSY.
     * Sqlite allows one writer at a time, so concurrent writes surface as SQLITE_BUSY.
     *
     * Waits asynchronously, unlike the driver's own busy timeout, so it does not block the
     * event loop while waiting.
     *
     * Statements inside a transaction are not retried: sqlite has already rolled the failed
     * statement back, so retrying just that one would commit a partial unit of work.
     * COMMIT and ROLLBACK are retried, since both can legitimately return SQLITE_BUSY.
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
     *
     * Sqlite has one connection, so query runners take turns holding it for the length of
     * their transaction. This wait has to outlast the holder's whole retry budget, or a
     * waiter gives up on a holder that is still making progress.
     *
     * Defaults to the worst-case retry budget scaled for a few contending runners,
     * and never less than 30000.
     */
    readonly connectionLeaseTimeout?: number
}
