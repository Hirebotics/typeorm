import { IsolationLevel } from "../types/IsolationLevel"
import { SqliteLeaseHolder } from "../sqlite-abstract/SqliteConnectionLease"
import { SqliteQueryRunner } from "./SqliteQueryRunner"

/**
 * SqliteQueryRunner serialized against the driver's single connection.
 * See SqliteConnectionLease.ts for the rationale.
 * Hirebotics file, not part of upstream TypeORM.
 */
export class SerializedSqliteQueryRunner extends SqliteQueryRunner {
    /**
     * Expose the inherited protected property so the connection lease can see it.
     */
    declare transactionDepth: number

    private leaseHolder = new SqliteLeaseHolder(this)

    async query(
        query: string,
        parameters?: any[],
        useStructuredResult = false,
    ): Promise<any> {
        return this.leaseHolder.run(query, (sql) => {
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
}
