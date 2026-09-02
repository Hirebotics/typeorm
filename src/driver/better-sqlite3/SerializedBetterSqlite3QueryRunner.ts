import { IsolationLevel } from "../types/IsolationLevel"
import { SqliteConnectionSerializer } from "../sqlite-abstract/SqliteConnectionSerializer"
import { BetterSqlite3QueryRunner } from "./BetterSqlite3QueryRunner"

/**
 * BetterSqlite3QueryRunner serialized against the driver's single connection.
 * See SqliteConnectionSerializer.ts for the rationale.
 * Hirebotics file, not part of upstream TypeORM.
 */
export class SerializedBetterSqlite3QueryRunner extends BetterSqlite3QueryRunner {
    private readonly serializer = new SqliteConnectionSerializer(this.driver)

    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        return this.serializer.run(query, (sql) => {
            return super.query(sql, parameters, useStructuredResult)
        })
    }

    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        try {
            await super.startTransaction(isolationLevel)
        } catch (err) {
            // Upstream leaves isTransactionActive set when the BEGIN fails,
            // and clears it when a nested begin fails while the outer transaction is open.
            // Both leave the flag disagreeing with sqlite, which is the authority.
            this.isTransactionActive = this.serializer.isTransactionOpen
            throw err
        }
    }

    async release(): Promise<void> {
        if (this.isReleased) {
            return
        }
        await this.serializer.rollbackOnRelease((sql) => {
            return super.query(sql)
        })
        // Nothing is open once released, whether or not a rollback was needed.
        this.isTransactionActive = false
        this.transactionDepth = 0
        this.isReleased = true
        await super.release()
    }

    /**
     * Routed through query() so serialization and the busy retry cover the pragma.
     * Upstream issues the migration and schema-loading pragmas through its driver's
     * pragma() rather than through query(), which would bypass our special handling.
     */
    async beforeMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = OFF`)
    }

    async afterMigration(): Promise<void> {
        await this.query(`PRAGMA foreign_keys = ON`)
    }

    /**
     * Routed through query() so serialization and the busy retry cover the pragma.
     * Reimplemented rather than delegated to super:
     * the abstract version drops the attached-database prefix,
     * so delegating would silently lose attached-database support.
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
