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
     * In your SQLite application when you perform parallel writes its common to face SQLITE_BUSY error.
     * This error indicates that SQLite failed to write to the database file since someone else already writes into it.
     * Since SQLite cannot handle parallel saves this error cannot be avoided.
     *
     * To simplify life's of those who have this error this particular option sets a timeout within which ORM will try
     * to perform requested write operation again and again until it receives SQLITE_BUSY error.
     *
     * Enabling WAL can improve your app performance and face less SQLITE_BUSY issues.
     * Time in milliseconds.
     */
    readonly busyErrorRetry?: number
}
