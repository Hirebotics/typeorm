import "reflect-metadata"
import { expect } from "chai"
import { DataSource } from "../../../../src"
import {
    closeTestingConnections,
    createTestingConnections,
} from "../../../utils/test-utils"
import { extendPostgresDriver } from "../../../../src/driver/postgres/PostgresDriverExtension"

describe("postgres driver > connection lifecycle hooks", () => {
    let connections: DataSource[]
    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [],
                enabledDrivers: ["postgres"],
            })),
    )
    after(() => closeTestingConnections(connections))
    // registeredOptions lives at module scope, so clear it after every test to
    // keep hooks from leaking into any other suite that talks to postgres.
    afterEach(() => extendPostgresDriver({}))

    it("runs onConnect once when a query runner is checked out and onRelease once when released", () =>
        Promise.all(
            connections.map(async (connection) => {
                const events: string[] = []
                let onConnectArg: unknown
                let onReleaseArg: unknown

                extendPostgresDriver({
                    onConnect: async (pg) => {
                        onConnectArg = pg
                        events.push("connect")
                    },
                    onRelease: async (pg) => {
                        onReleaseArg = pg
                        events.push("release")
                    },
                })

                const queryRunner = connection.createQueryRunner()
                try {
                    await queryRunner.connect()
                    // A query reuses the already-checked-out client, so it must
                    // NOT trigger the checkout hook a second time.
                    await queryRunner.query("SELECT 1")
                    await queryRunner.query("SELECT 1")

                    expect(events).to.eql(["connect"])
                    expect(onConnectArg, "onConnect receives the raw pg client")
                        .to.exist
                } finally {
                    await queryRunner.release()
                }

                expect(events).to.eql(["connect", "release"])
                expect(onReleaseArg, "onRelease receives the raw pg client").to
                    .exist
            }),
        ))

    it("runs no hooks once they are cleared", () =>
        Promise.all(
            connections.map(async (connection) => {
                let ran = false
                extendPostgresDriver({
                    onConnect: async () => {
                        ran = true
                    },
                    onRelease: async () => {
                        ran = true
                    },
                })
                extendPostgresDriver({}) // clears the hooks

                const queryRunner = connection.createQueryRunner()
                try {
                    await queryRunner.connect()
                } finally {
                    await queryRunner.release()
                }

                expect(ran).to.equal(false)
            }),
        ))

    it("waits for onConnect before any concurrent statement runs", () =>
        Promise.all(
            connections.map(async (connection) => {
                // super.connect() publishes its connection before the hook can
                // run, so a second caller could otherwise be handed a client
                // whose session state was still being established.
                const events: string[] = []
                extendPostgresDriver({
                    onConnect: async () => {
                        events.push("hook:start")
                        await new Promise((ok) => {
                            setTimeout(ok, 100)
                        })
                        events.push("hook:end")
                    },
                })

                const queryRunner = connection.createQueryRunner()
                try {
                    await Promise.all([
                        queryRunner.query("SELECT 1").then(() => {
                            events.push("first:done")
                        }),
                        queryRunner.query("SELECT 2").then(() => {
                            events.push("second:done")
                        }),
                    ])

                    expect(events.indexOf("hook:end")).to.be.lessThan(
                        events.indexOf("first:done"),
                    )
                    expect(events.indexOf("hook:end")).to.be.lessThan(
                        events.indexOf("second:done"),
                    )
                } finally {
                    await queryRunner.release()
                }
            }),
        ))

    it("swallows a throwing onConnect so the connection stays usable", () =>
        Promise.all(
            connections.map(async (connection) => {
                // Production behaviour, unchanged. Making this fail the request is a
                // packages/server decision: today the hook swallows its own errors,
                // and RLS already yields no rows when the tenant is unset.
                extendPostgresDriver({
                    onConnect: async () => {
                        throw new Error("onConnect boom")
                    },
                })

                const queryRunner = connection.createQueryRunner()
                try {
                    await queryRunner.connect()
                    const result = await queryRunner.query("SELECT 1 AS ok")
                    expect(result[0].ok).to.equal(1)
                } finally {
                    await queryRunner.release()
                }
            }),
        ))

    it("does not return the client to the pool while the cleanup hook is still running", () =>
        Promise.all(
            connections.map(async (connection) => {
                // A second release() must stay on the hook path. If it reaches
                // super.release() first, the client is re-pooled while the cleanup
                // is in flight and the next borrower inherits this session's state.
                extendPostgresDriver({
                    onRelease: async () => {
                        await new Promise((ok) => {
                            setTimeout(ok, 100)
                        })
                    },
                })

                const queryRunner = connection.createQueryRunner()
                await queryRunner.query("SELECT 1")

                const nextTick = () =>
                    new Promise((ok) => {
                        setTimeout(ok, 10)
                    })

                const first = queryRunner.release()
                await nextTick()
                const second = queryRunner.release()
                await nextTick()

                // isReleased flips only once the client has gone back to the pool.
                const isReleasedDuringCleanup = queryRunner.isReleased

                await Promise.all([first, second])

                expect(isReleasedDuringCleanup).to.equal(false)
                expect(queryRunner.isReleased).to.equal(true)
            }),
        ))
})
