import { IsolationLevel } from "../types/IsolationLevel"
import {
    SqliteLeaseHolder,
    toImmediateBegin,
} from "../sqlite-abstract/SqliteConnectionLease"
import { BetterSqlite3QueryRunner } from "./BetterSqlite3QueryRunner"

/**
 * BetterSqlite3QueryRunner serialized against the driver's single connection.
 * See SqliteConnectionLease.ts for the rationale.
 */
export class SerializedBetterSqlite3QueryRunner extends BetterSqlite3QueryRunner {
    /**
     * The inherited protected property, widened to public.
     */
    declare transactionDepth: number

    private leaseHolder = new SqliteLeaseHolder(this)

    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        const sql = toImmediateBegin(query)
        return this.leaseHolder.run(sql, () => {
            return super.query(sql, parameters, useStructuredResult)
        })
    }

    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        try {
            await super.startTransaction(isolationLevel)
        } catch (err) {
            this.leaseHolder.releaseAfterFailedBegin()
            throw err
        }
    }

    async commitTransaction(): Promise<void> {
        try {
            await super.commitTransaction()
        } finally {
            this.leaseHolder.releaseIfIdle()
        }
    }

    async rollbackTransaction(): Promise<void> {
        try {
            await super.rollbackTransaction()
        } finally {
            this.leaseHolder.releaseIfIdle()
        }
    }

    async release(): Promise<void> {
        return this.leaseHolder.releaseRunner(() => {
            return super.release()
        })
    }

    /**
     * Routed through query() so the lease and the retry cover the pragma. Upstream
     * calls databaseConnection.pragma() directly, which skips both and blocks the
     * event loop inside better-sqlite3's synchronous busy timeout.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Same reason. Reimplemented rather than delegated to super: the abstract version
     * drops the attached-database prefix, so delegating would silently lose
     * attached-database support.
     */
    protected async loadPragmaRecords(
        tablePath: string,
        pragma: string,
    ): Promise<any> {
        const [database, tableName] = this.splitTablePath(tablePath)
        let prefix = ""
        if (database) {
            prefix = `"${database}".`
        }
        return this.query(`PRAGMA ${prefix}${pragma}("${tableName}")`)
    }
}
