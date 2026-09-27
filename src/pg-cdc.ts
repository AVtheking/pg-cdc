import { Context, Effect, Layer, Match, Stream } from "effect";
import pg from "pg";
import * as PgReplicator from "pg-replicator";
import { CDCChange, CDCTransaction } from "./types";
import { PostgresConnectionError } from "./error";

export interface Config {
    connectionString: string;
    publication: string;
    slot: string
}

export interface PostgresCDCService {
    transaction: Stream.Stream<CDCTransaction, PgReplicator.PgReplError>
    changes: Stream.Stream<CDCChange, PgReplicator.PgReplError>
}

interface State {
    relations: Map<number, { schema: string, table: string }>
    currentTransaction: { xid: number, changes: CDCChange[] }
}

type Step = readonly [State, ReadonlyArray<CDCTransaction>]

export class PostgresCDC extends Context.Service<PostgresCDC, PostgresCDCService>()("PostgresCDC") { }

export const make = Effect.fn(function* (config: Config) {

    const client = yield* Effect.acquireRelease(
        Effect.tryPromise({
            try: async () => {
                const client = new pg.Client({
                    connectionString: config.connectionString,
                    replication: "database"
                } as pg.ClientConfig)
                await client.connect()
                return client
            },
            catch: (error) =>
                new PostgresConnectionError(error instanceof Error ? error.message : "Unknown error")

        }),
        (client) => Effect.promise(() => client.end())
    )

    const pgReplicator = yield* PgReplicator.fromPg(client)


    const startLSN = yield* pgReplicator.createReplicationSlot({
        slotName: config.slot,
        outputPlugin: "pgoutput",
    }).pipe(
        Effect.map((slot) => slot.consistentPoint),
        Effect.catchTag("SlotAlreadyExists", () =>
            Effect.logInfo(`Slot ${config.slot} already exists`).pipe(Effect.as(0n))
        ),
    )

    const toChange = (rel: { schema: string, table: string }, msg: PgReplicator.PgOutput) =>
        Match.value(msg).pipe(
            Match.withReturnType<CDCChange | undefined>(),
            Match.tag("Insert", (insert) => {
                return CDCChange.Insert({
                    schema: rel.schema,
                    table: rel.table,
                    after: insert.rows!
                })
            }),
            Match.tag("Update", (update) => {
                return CDCChange.Update({
                    schema: rel.schema,
                    table: rel.table,
                    before: update.oldRows ? {
                        kind: update.oldTupleKind === "O" ? "full" : "key", rows: update.oldRows
                    } : undefined,
                    after: update.newRows!
                })
            }),
            Match.tag("Delete", (del) => {
                return CDCChange.Delete({
                    schema: rel.schema,
                    table: rel.table,
                    before: del.rows ? {
                        kind: del.oldTupleKind === "O" ? "full" : "key", rows: del.rows
                    } : undefined
                })
            }),
            Match.orElse(() => undefined)
        )


    return {
        transaction: pgReplicator.startReplication({
            slot: config.slot,
            publication: config.publication,
            startLSN,
            protoVersion: 2
        }).pipe(
            Stream.mapAccum(
                (): State => ({
                    relations: new Map(),
                    currentTransaction: { xid: 0, changes: [] }
                }),
                (state, msg): Step => {
                    switch (msg._tag) {
                        case "Relation":
                            state.relations.set(msg.relationId, { schema: msg.namespace, table: msg.name })
                            return [state, []]
                        case "Begin":
                            state.currentTransaction = { xid: msg.xid, changes: [] }
                            return [state, []]
                        case "Insert":
                        case "Update":
                        case "Delete":
                            const relation = state.relations.get(msg.relationId)
                            if (!relation) return [state, []] // TODO: raise error here

                            const change = toChange(relation, msg)
                            if (!change) return [state, []]

                            state.currentTransaction.changes.push(change)
                            return [state, []]
                        case "Commit":
                            const xid = state.currentTransaction.xid
                            const changes = state.currentTransaction.changes
                            if (!changes) return [state, []] // TODO: raise error here
                            return [state, [{
                                xid,
                                commitLSN: PgReplicator.formatLSN(msg.commitLSN),
                                changes,
                                acknowledge: pgReplicator.ack(msg.endLSN)
                            }]]

                        default:
                            return [state, []]
                    }
                }

            )
        ),
        changes: Stream.die(new Error("Not implemented")),
    }
})

export const layer = (config: Config) => Layer.effect(PostgresCDC, make(config))