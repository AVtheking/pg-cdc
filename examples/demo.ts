import { Effect, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import * as PostgresCDC from "../src/pg-cdc.ts"

const program = Effect.fn(function* () {
    const cdc = yield* PostgresCDC.make({
        connectionString: "postgres://postgres:postgres@localhost:5432/postgres",
        publication: "my_pub",
        slot: "my_slot",
    })

    yield* cdc.transaction.pipe(
        Stream.runForEach((tx) =>
            Effect.gen(function* () {
                //publishToKafka()
                yield* Effect.logInfo(`Transaction: ${tx.xid}`)
                yield* Effect.logInfo(`Changes: ${tx.changes.map(c => c._tag).join(", ")}`)
            }))
    )
})

program().pipe(
    Effect.scoped,
    NodeRuntime.runMain
)