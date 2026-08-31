import { IsolationLevel } from "../types/IsolationLevel"
import {
    SqliteLeaseHolder,
    toImmediateBegin,
} from "../sqlite-abstract/SqliteConnectionLease"
import { SqliteQueryRunner } from "./SqliteQueryRunner"

/**
 * SqliteQueryRunner serialized against the driver's single connection.
 * See SqliteConnectionLease.ts for the rationale.
 */
export class SerializedSqliteQueryRunner extends SqliteQueryRunner {
    /** The inherited protected property, widened to public. */
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
}
