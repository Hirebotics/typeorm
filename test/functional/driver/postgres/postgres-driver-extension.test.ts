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

    it("swallows a throwing onConnect so the connection stays usable", () =>
        Promise.all(
            connections.map(async (connection) => {
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
})
