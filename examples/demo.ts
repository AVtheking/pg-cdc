import { Effect, Logger, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import * as PostgresCDC from "pg-cdc"

const program = Effect.gen(function* () {
    const cdc = yield* PostgresCDC.make({
        connectionString: "postgres://postgres:postgres@localhost:5432/postgres",
        publication: "my_pub",
        slot: "my_slot",
    })

    // yield* cdc.changes.pipe(
    //     Stream.filter((ev) => ev.table === "todos"),
    //     Stream.runForEach((ev) => Effect.gen(function* () {
    //         yield* Effect.logInfo(`Change from change stream: ${JSON.stringify(ev, null, 2)}`)
    //     }))
    // )

    yield* cdc.transaction.pipe(
        Stream.runForEach((tx) =>
            Effect.gen(function* () {
                //publishToKafka()
                //sendMessageToClickhouse()
                yield* Effect.logInfo(`Transaction: ${tx.xid} commitLSN: ${tx.commitLSN}`)
                yield* Effect.logInfo(JSON.stringify(tx.changes, null, 2))
                yield* tx.acknowledge
            }))
    )
})

program.pipe(
    Effect.scoped,
    Effect.provide(Logger.layer([Logger.consolePretty()])),
    NodeRuntime.runMain
)
