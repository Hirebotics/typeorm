import { PostgresQueryRunner } from "./PostgresQueryRunner"

/**
 * Hooks that run when a postgres connection is checked out of the pool and handed back.
 *
 * The argument is the raw `pg` client the pool handed out. It is typed loosely because
 * typeorm does not depend on `pg`'s types.
 */
export interface PostgresExtensionOptions {
    onConnect?: (pg: any) => Promise<void>
    onRelease?: (pg: any) => Promise<void>
}

/**
 * Module-level, because the hooks have to be in place before typeorm builds any driver.
 * Calling extendPostgresDriver() again replaces them.
 */
let registeredOptions: PostgresExtensionOptions | undefined

/**
 * Query runner that runs the registered hooks around the pooled connection's lifetime.
 *
 * Use case: session-scoped state, such as a `SET app.current_tenant` on checkout that
 * must be reset before the connection returns to the pool.
 */
export class PostgresQueryRunnerExtension extends PostgresQueryRunner {
    private rawConnection: any

    async connect(): Promise<any> {
        // super.connect() checks a client out of the pool only on the first call,
        // afterwards it returns the memoized connection.
        // query() calls connect() for every query, so guard the hook to fire
        // once per real checkout rather than once per query.
        const alreadyConnected = !!(
            this.databaseConnection || this.databaseConnectionPromise
        )

        this.rawConnection = await super.connect()

        if (
            !alreadyConnected &&
            this.rawConnection &&
            registeredOptions?.onConnect
        ) {
            try {
                await registeredOptions.onConnect(this.rawConnection)
            } catch (err) {
                // Never fail the checkout: the connection itself is usable.
                this.connection.logger.log(
                    "warn",
                    `Postgres onConnect extension failed. ${err}`,
                    this,
                )
            }
        }

        return this.rawConnection
    }

    async release(): Promise<void> {
        if (!this.isReleased && this.rawConnection) {
            if (registeredOptions?.onRelease) {
                try {
                    await registeredOptions.onRelease(this.rawConnection)
                } catch (err) {
                    // Swallowed so the connection is still returned to the pool.
                    this.connection.logger.log(
                        "warn",
                        `Postgres onRelease extension failed. ${err}`,
                        this,
                    )
                }
            }

            this.rawConnection = undefined
        }

        await super.release()
    }
}

/**
 * Registers the hooks that every postgres query runner will then run.
 */
export const extendPostgresDriver = (
    options: PostgresExtensionOptions,
): void => {
    registeredOptions = {
        ...options,
    }
}
