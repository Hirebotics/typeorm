import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { AbstractSqliteQueryRunner } from "../sqlite-abstract/AbstractSqliteQueryRunner"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BetterSqlite3Driver } from "./BetterSqlite3Driver"
import { QueryResult } from "../../query-runner/QueryResult"
import { BetterSqlite3ConnectionOptions } from "./BetterSqlite3ConnectionOptions"
import { QueryFailedError } from "../../error/QueryFailedError"
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
                    const key = this.stmtCache.keys().next().value
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
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    /**
     * Called after migrations are run.
     */
    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Executes a given SQL query.
     */
    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const connection = this.driver.connection
        const options = connection.options as BetterSqlite3ConnectionOptions
        const maxQueryExecutionTime = this.driver.options.maxQueryExecutionTime
        const broadcasterResult = new BroadcasterResult()
        const broadcaster = this.broadcaster

        const busyErrorRetryInterval = options.busyErrorRetryInterval || 0
        const busyErrorRetryLimit = options.busyErrorRetryLimit || 0
        let busyErrorRetryCount = 0

        broadcaster.broadcastBeforeQueryEvent(
            broadcasterResult,
            query,
            parameters,
        )

        return new Promise(async (ok, fail) => {
            try {
                const self = this
                const queryStartTime = Date.now()
                this.driver.connection.logger.logQuery(query, parameters, this)

                const execute = async () => {
                    try {
                        const stmt = await this.getStmt(query)
                        const result = new QueryResult()

                        if (stmt.reader) {
                            const raw = stmt.all(...(parameters || []))

                            result.raw = raw

                            if (Array.isArray(raw)) {
                                result.records = raw
                            }

                            handler(null, result)
                        } else {
                            const raw = stmt.run(...(parameters || []))
                            result.affected = raw.changes
                            result.raw = raw.lastInsertRowid

                            handler(null, result)
                        }
                    } catch (err) {
                        handler(err, null)
                    }
                }

                const failQuery = (err: Error) => {
                    connection.logger.logQueryError(
                        err,
                        query,
                        parameters,
                        self,
                    )
                    broadcaster.broadcastAfterQueryEvent(
                        broadcasterResult,
                        query,
                        parameters,
                        false,
                        undefined,
                        undefined,
                        err,
                    )
                    fail(new QueryFailedError(query, parameters, err))
                }

                const handler = function (
                    err: Error | null,
                    result: QueryResult | null,
                ) {
                    if (
                        busyErrorRetryInterval > 0 &&
                        self.isSqliteError(err, "SQLITE_BUSY")
                    ) {
                        busyErrorRetryCount++
                        if (
                            busyErrorRetryLimit > 0 &&
                            busyErrorRetryCount > busyErrorRetryLimit
                        ) {
                            connection.logger.log(
                                "warn",
                                `Sqlite is busy, but retry limit reached, failing query`,
                            )
                            failQuery(err)
                            return
                        }
                        connection.logger.log(
                            "info",
                            `Sqlite is busy, retrying query after ${busyErrorRetryInterval}ms (attempt ${busyErrorRetryCount} of ${busyErrorRetryLimit})`,
                        )
                        setTimeout(execute, busyErrorRetryInterval)
                        return
                    }

                    // log slow queries if maxQueryExecution time is set
                    const queryEndTime = Date.now()
                    const queryExecutionTime = queryEndTime - queryStartTime
                    if (
                        maxQueryExecutionTime &&
                        queryExecutionTime > maxQueryExecutionTime
                    ) {
                        connection.logger.logQuerySlow(
                            queryExecutionTime,
                            query,
                            parameters,
                            self,
                        )
                    }

                    if (err) {
                        failQuery(err)
                    } else if (result) {
                        broadcaster.broadcastAfterQueryEvent(
                            broadcasterResult,
                            query,
                            parameters,
                            true,
                            queryExecutionTime,
                            result.raw,
                            undefined,
                        )

                        if (useStructuredResult) {
                            ok(result)
                        } else {
                            ok(result.raw)
                        }
                    }
                }

                await execute()
            } catch (err) {
                fail(err)
            } finally {
                await broadcasterResult.wait()
            }
        })
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

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
        const res = await this.query(
            `PRAGMA ${
                database ? `"${database}".` : ""
            }${pragma}("${tableName}")`,
        )
        return res
    }

    /**
     * Check if the error is a specific sqlite error.
     * Returns true if the error's `code` is the string to find,
     * or if the error's `message` contains the string to find.
     *
     * https://github.com/WiseLibs/better-sqlite3/blob/master/lib/sqlite-error.js
     *
     * Example:
     * ```
     * [SqliteError] {
     *   code: 'SQLITE_BUSY',
     *   message: 'database is locked',
     *   toString: 'SqliteError: database is locked',
     * }
     *
     * isSqliteError(err, 'SQLITE_BUSY')         // true
     * isSqliteError(err, 'database is locked')  // true
     * isSqliteError(err, 'lorem ipsum')         // false
     * ```
     */
    protected isSqliteError(
        err: (Error & { code?: string }) | null,
        stringToFind: string,
    ): err is Error {
        if (!err) {
            return false
        }
        if (err.code === stringToFind) {
            return true
        }
        if (err.toString().indexOf(stringToFind) !== -1) {
            return true
        }
        return false
    }
}
