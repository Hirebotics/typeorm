import { IsolationLevel } from "../types/IsolationLevel"
import { SqliteConnectionSerializer } from "../sqlite-abstract/SqliteConnectionSerializer"
import { SqliteQueryRunner } from "./SqliteQueryRunner"

/**
 * SqliteQueryRunner serialized against the driver's single connection.
 * See SqliteConnectionSerializer.ts for the rationale.
 * Hirebotics file, not part of upstream TypeORM.
 *
 * This runner overrides fewer methods than the better-sqlite3 one.
 * Upstream routes every statement here through query(), including the migration
 * and schema-loading pragmas, so overriding query() serializes all of them.
 * The better-sqlite3 runner calls its driver's pragma() directly in those three
 * places, which is why that runner has to redirect them.
 */
export class SerializedSqliteQueryRunner extends SqliteQueryRunner {
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
}
