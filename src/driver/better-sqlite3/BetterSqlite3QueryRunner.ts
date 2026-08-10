import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { QueryFailedError } from "../../error/QueryFailedError"
import { AbstractSqliteQueryRunner } from "../sqlite-abstract/AbstractSqliteQueryRunner"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BetterSqlite3Driver } from "./BetterSqlite3Driver"
import { QueryResult } from "../../query-runner/QueryResult"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"

/**
 * Runs queries on a single sqlite database connection.
 *
 * Does not support compose primary keys with autoincrement field.
 * todo: need to throw exception for this case.
 */
export class BetterSqlite3QueryRunner extends AbstractSqliteQueryRunner {
    /**
     * Database driver used by connection.
     */
    driver: BetterSqlite3Driver

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: BetterSqlite3Driver) {
        super()
        this.driver = driver
        this.connection = driver.connection
        this.broadcaster = new Broadcaster(this)
        if (typeof this.driver.options.statementCacheSize === "number") {
            this.cacheSize = this.driver.options.statementCacheSize
        } else {
            this.cacheSize = 100
        }
    }

    private cacheSize: number
    private stmtCache = new Map<string, any>()

    private async getStmt(query: string) {
        if (this.cacheSize > 0) {
            let stmt = this.stmtCache.get(query)
            if (!stmt) {
                const databaseConnection = await this.connect()
                stmt = databaseConnection.prepare(query)
                this.stmtCache.set(query, stmt)
                while (this.stmtCache.size > this.cacheSize) {
                    // since es6 map keeps the insertion order,
                    // it comes to be FIFO cache
                    const key = this.stmtCache.keys().next().value!
                    this.stmtCache.delete(key)
                }
            }
            return stmt
        } else {
            const databaseConnection = await this.connect()
            return databaseConnection.prepare(query)
        }
    }

    /**
     * Called before migrations are run.
     */
    async beforeMigration(): Promise<void> {
        // Routed through query() on purpose, so the SQLITE_BUSY retry covers it.
        // Migrations can start while another writer already holds the database.
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        // Routed through query() on purpose -- see beforeMigration().
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Executes a given SQL query.
     */
    async query(
        query: string,
        parameters: any[] = [],
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const connection = this.driver.connection

        const broadcasterResult = new BroadcasterResult()

        this.driver.connection.logger.logQuery(query, parameters, this)
        this.broadcaster.broadcastBeforeQueryEvent(
            broadcasterResult,
            query,
            parameters,
        )
        const queryStartTime = Date.now()

        try {
            const result = await this.executeWithBusyRetry(query, parameters)

            // log slow queries if maxQueryExecution time is set
            const maxQueryExecutionTime =
                this.driver.options.maxQueryExecutionTime
            const queryEndTime = Date.now()
            const queryExecutionTime = queryEndTime - queryStartTime
            if (
                maxQueryExecutionTime &&
                queryExecutionTime > maxQueryExecutionTime
            )
                connection.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )

            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                true,
                queryExecutionTime,
                result.raw,
                undefined,
            )

            if (!useStructuredResult) {
                return result.raw
            }

            return result
        } catch (err) {
            connection.logger.logQueryError(err, query, parameters, this)
            this.broadcaster.broadcastAfterQueryEvent(
                broadcasterResult,
                query,
                parameters,
                false,
                undefined,
                undefined,
                err,
            )

            throw new QueryFailedError(query, parameters, err)
        } finally {
            // Every other driver waits here.
            // Caveat: a subscriber that rejects replaces the real query error.
            await broadcasterResult.wait()
        }
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Runs the query, retrying while sqlite reports the database is busy.
     *
     * A retry loop rather than the `timeout` option (sqlite3_busy_timeout) because
     * better-sqlite3 is synchronous, so a busy timeout blocks the whole event loop --
     * and sqlite never invokes the busy handler for SQLITE_BUSY_SNAPSHOT anyway.
     *
     * The broadcaster events stay in query() so they fire once per query, not once per attempt.
     */
    protected async executeWithBusyRetry(
        query: string,
        parameters: any[],
    ): Promise<QueryResult> {
        const { busyErrorRetryInterval = 0, busyErrorRetryLimit = 0 } =
            this.driver.options

        for (let attempt = 0; ; attempt++) {
            try {
                // getStmt() is inside the loop because prepare() reads the schema,
                // so it can raise SQLITE_BUSY too.
                return this.executeStmt(await this.getStmt(query), parameters)
            } catch (err) {
                // Retry is opt-in: with no interval configured, behave like upstream.
                if (
                    busyErrorRetryInterval <= 0 ||
                    !this.isSqliteError(err, "SQLITE_BUSY")
                )
                    throw err

                // SQLITE_BUSY_SNAPSHOT means the transaction's WAL snapshot went stale,
                // and sqlite keeps that snapshot for the life of the transaction.
                // Every retry then fails identically, so spend none of the budget on them.
                // SQLITE_BUSY_RECOVERY is not treated this way: another process is rebuilding
                // the WAL, which ends on its own, so retrying does clear it.
                if (
                    this.isSqliteError(err, "SQLITE_BUSY_SNAPSHOT") &&
                    this.isTransactionOpen()
                ) {
                    this.driver.connection.logger.log(
                        "warn",
                        "SQLITE_BUSY_SNAPSHOT inside an open transaction cannot be resolved by retrying, " +
                            "the transaction must be rolled back and replayed. Failing query immediately",
                        this,
                    )

                    throw err
                }

                // A falsy limit means retry forever, matching the sqlite driver's busyErrorRetry.
                if (busyErrorRetryLimit > 0 && attempt >= busyErrorRetryLimit) {
                    this.driver.connection.logger.log(
                        "warn",
                        `Sqlite is busy and the retry limit of ${busyErrorRetryLimit} is reached, failing query`,
                        this,
                    )

                    // Reject with the sqlite error itself, not a synthetic "gave up" error.
                    throw err
                }

                this.driver.connection.logger.log(
                    "info",
                    `Sqlite is busy, retrying query in ${busyErrorRetryInterval}ms (retry ${
                        attempt + 1
                    } of ${busyErrorRetryLimit || "unlimited"})`,
                    this,
                )
                await new Promise((ok) =>
                    setTimeout(ok, busyErrorRetryInterval),
                )
            }
        }
    }

    /**
     * True when the sqlite connection sits inside an open transaction.
     *
     * Asks better-sqlite3 as well as this runner: every sqlite query runner shares the one
     * driver connection, so a transaction another runner opened -- or a raw `BEGIN`, which
     * never sets isTransactionActive -- pins a snapshot for this runner's queries too.
     */
    protected isTransactionOpen(): boolean {
        return (
            this.isTransactionActive ||
            this.driver.databaseConnection?.inTransaction === true
        )
    }

    /**
     * True when `err` is a better-sqlite3 SqliteError whose code starts with `codePrefix`.
     *
     * Prefix and not equality, because SQLITE_BUSY_SNAPSHOT and SQLITE_BUSY_RECOVERY are
     * separate codes that still mean "database is busy".
     *
     * @see https://github.com/WiseLibs/better-sqlite3/blob/master/lib/sqlite-error.js
     */
    protected isSqliteError(err: any, codePrefix: string): boolean {
        return typeof err?.code === "string" && err.code.startsWith(codePrefix)
    }

    protected executeStmt(stmt: any, parameters: any[]): QueryResult {
        const result = new QueryResult()

        if (stmt.reader) {
            const raw = stmt.all(...parameters)

            result.raw = raw

            if (Array.isArray(raw)) {
                result.records = raw
            }
        } else {
            const raw = stmt.run(...parameters)
            result.affected = raw.changes
            result.raw = raw.lastInsertRowid
        }

        return result
    }

    protected async loadTableRecords(
        tablePath: string,
        tableOrIndex: "table" | "index",
    ) {
        const [database, tableName] = this.splitTablePath(tablePath)
        const res = await this.query(
            `SELECT ${
                database ? `'${database}'` : null
            } as database, * FROM ${this.escapePath(
                `${database ? `${database}.` : ""}sqlite_master`,
            )} WHERE "type" = '${tableOrIndex}' AND "${
                tableOrIndex === "table" ? "name" : "tbl_name"
            }" IN ('${tableName}')`,
        )
        return res
    }
    protected async loadPragmaRecords(tablePath: string, pragma: string) {
        const [database, tableName] = this.splitTablePath(tablePath)
        // Routed through query() on purpose -- see beforeMigration().
        // databaseConnection.pragma() bypasses the retry and falls back to
        // better-sqlite3's synchronous timeout, which blocks the event loop.
        const res = await this.query(
            `PRAGMA ${
                database ? `"${database}".` : ""
            }${pragma}("${tableName}")`,
        )
        return res
    }
}
