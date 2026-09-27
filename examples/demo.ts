import { Effect, Logger, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import * as PostgresCDC from "../src/pg-cdc.ts"

const program = Effect.fn(function* () {
    const cdc = yield* PostgresCDC.make({
        connectionString: "postgres://postgres:postgres@localhost:5434/syncengine",
        publication: "sync_pub",
        slot: "my_slot",
    })

    yield* cdc.transaction.pipe(
        Stream.runForEach((tx) =>
            Effect.gen(function* () {
                //publishToKafka()
                yield* Effect.logInfo(`Transaction: ${tx.xid} commitLSN: ${tx.commitLSN}`)
                yield* Effect.logInfo(JSON.stringify(tx.changes, null, 2))
                yield* tx.acknowledge
            }))
    )
})

program().pipe(
    Effect.scoped,
    Effect.provide(Logger.layer([Logger.consolePretty()])),
    NodeRuntime.runMain
)