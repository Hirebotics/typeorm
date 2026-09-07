import { PostgresQueryRunner } from "./PostgresQueryRunner"

/**
 * Per-checkout hooks for the postgres connection pool.
 * Hirebotics file, not part of upstream TypeORM.
 */

/**
 * Hooks that run when a postgres connection is checked out of the pool and handed back.
 *
 * The argument is the raw `pg` client the pool handed out.
 * It is typed loosely because typeorm does not depend on `pg`'s types.
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
 * Use case: session-scoped state,
 * such as a `SET app.current_tenant` on checkout
 * that must be reset before the connection returns to the pool.
 *
 * The `onRelease` option must clear everything `onConnect` set
 * because it can run in a different async context from `onConnect`,
 * so it cannot decide what to clear by reading ambient request state.
 * A skipped cleanup leaks state to the next borrower.
 */
export class PostgresQueryRunnerExtension extends PostgresQueryRunner {
    private rawConnection: any

    /**
     * Resolves only once the pool checkout *and* onConnect have both finished.
     */
    private checkoutPromise: Promise<any> | undefined

    async connect(): Promise<any> {
        if (!this.checkoutPromise) {
            this.checkoutPromise = this.checkoutWithHook()
        }
        return this.checkoutPromise
    }

    async release(): Promise<void> {
        if (!this.isReleased && this.rawConnection) {
            if (registeredOptions?.onRelease) {
                try {
                    await registeredOptions.onRelease(this.rawConnection)
                } catch (err) {
                    // The client goes back to the pool either way, so a failed
                    // cleanup is only logged.
                    this.connection.logger.log(
                        "warn",
                        `Postgres onRelease extension failed. ${err}`,
                        this,
                    )
                }
            }

            // Cleared only after the hook settles. Clearing it first sends a
            // concurrent release() down the early return, which re-pools the
            // client while the cleanup is still in flight.
            this.rawConnection = undefined
        }

        await super.release()
    }

    /**
     * Checks a client out of the pool and runs onConnect before anyone sees it.
     */
    private async checkoutWithHook(): Promise<any> {
        this.rawConnection = await super.connect()

        if (registeredOptions?.onConnect) {
            try {
                await registeredOptions.onConnect(this.rawConnection)
            } catch (err) {
                // The client is usable, only its session state is missing.
                this.connection.logger.log(
                    "warn",
                    `Postgres onConnect extension failed. ${err}`,
                    this,
                )
            }
        }

        return this.rawConnection
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
