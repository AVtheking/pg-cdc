import { Context, Effect, Layer, Logger, Stream } from "effect";
import { NodeRuntime } from "@effect/platform-node";
import { PostgresCDC, layer } from "pg-cdc";
import type { CDCTransaction } from "pg-cdc";

class KafkaProducer extends Context.Service<KafkaProducer, {
    readonly send: (topic: string, tx: CDCTransaction) => Effect.Effect<void>
}>()("examples/KafkaProducer") {}

const KafkaProducerMock = Layer.succeed(KafkaProducer, {
    send: (topic, tx) =>
        Effect.logInfo(`kafka ${topic} xid=${tx.xid} ${JSON.stringify(tx.changes)}`)
})

const program = Effect.gen(function* () {
    const cdc = yield* PostgresCDC
    const producer = yield* KafkaProducer

    yield* cdc.transaction.pipe(
        Stream.runForEach((tx) =>
            producer.send("todos", tx).pipe(Effect.andThen(tx.acknowledge)))
    )
})

program.pipe(
    Effect.provide(Layer.mergeAll(
        layer({
            connectionString: "postgres://postgres:postgres@localhost:5432/postgres",
            publication: "my_pub",
            slot: "my_slot",
        }),
        KafkaProducerMock,
        Logger.layer([Logger.consolePretty()])
    )),
    NodeRuntime.runMain
)
