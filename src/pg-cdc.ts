import { Context, Effect, Layer, Match, Stream } from "effect";
import pg from "pg";
import * as PgReplicator from "pg-replicator";
import { CDCChange, type CDCTransaction } from "./types.ts";
import { PostgresConnectionError, PostgresQueryError, RelationNotFound } from "./error.ts";

interface Publication {
    name: string;
    tables: string[];
    createIfMissing: boolean;
}

export interface Config {
    connectionString: string;
    publication: string | Publication;
    slot: string
    // replicaIdentity: "full" | "key";
}

interface State {
    relations: Map<number, { schema: string, table: string }>
    currentTransaction: { xid: number, changes: CDCChange[] }
}

type Step = readonly [State, ReadonlyArray<CDCTransaction>]

export interface PostgresCDCService {
    transaction: Stream.Stream<CDCTransaction, PgReplicator.PgReplError | RelationNotFound>
    changes: Stream.Stream<CDCChange, PgReplicator.PgReplError | RelationNotFound>
}

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

const setupPublication = Effect.fn(function* (client: pg.Client, publication: string | Publication) {
    const sql = (query: string) =>
        Effect.tryPromise({
            try: () => client.query(query),
            catch: (error) => new PostgresQueryError({
                message: `Failed to execute query: ${query}`,
                cause: error
            })
        })

    if (typeof publication === "string") {
        return publication;
    }

    const existingPublication = yield* sql(`SELECT 1 FROM pg_publication WHERE pubname = ${pg.escapeLiteral(publication.name)}`)
    if (existingPublication.rows.length > 0) return publication.name;

    const tables = publication.tables.map((table) => table.split(".").map(pg.escapeIdentifier).join("."))
    yield* sql(`CREATE PUBLICATION ${pg.escapeIdentifier(publication.name)} FOR TABLE ${tables.join(", ")}`)

    yield* Effect.logInfo(`Created publication ${publication.name}`)
    return publication.name;
})

export class PostgresCDC extends Context.Service<PostgresCDC, PostgresCDCService>()("pg-cdc/PostgresCDC") { }

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
                new PostgresConnectionError({
                    message: error instanceof Error ? error.message : "Unknown error",
                    cause: error
                })
        }),
        (client) => Effect.promise(() => client.end())
    )

    const pgReplicator = yield* PgReplicator.fromPg(client)
    const publication = yield* setupPublication(client, config.publication)
    const startLSN = yield* pgReplicator.createReplicationSlot({
        slotName: config.slot,
        outputPlugin: "pgoutput",
    }).pipe(
        Effect.map((slot) => slot.consistentPoint),
        Effect.catchTag("SlotAlreadyExists", () =>
            Effect.logInfo(`Slot ${config.slot} already exists`).pipe(Effect.as(0n))
        ),
    )

    const transaction = pgReplicator.startReplication({
        slot: config.slot,
        publication,
        startLSN,
        protoVersion: 2
    }).pipe(
        Stream.mapAccumEffect(
            (): State => ({
                relations: new Map(),
                currentTransaction: { xid: 0, changes: [] }
            }),
            (state, msg): Effect.Effect<Step, RelationNotFound> => {
                switch (msg._tag) {
                    case "Keepalive":
                        return Effect.succeed([state, []])
                    case "XLogData":
                        const xlogData = msg.message
                        switch (xlogData._tag) {
                            case "Relation":
                                state.relations.set(xlogData.relationId, { schema: xlogData.namespace, table: xlogData.name })
                                return Effect.succeed([state, []])
                            case "Begin":
                                state.currentTransaction = { xid: xlogData.xid, changes: [] }
                                return Effect.succeed([state, []])
                            case "Insert":
                            case "Update":
                            case "Delete":
                                const relation = state.relations.get(xlogData.relationId)
                                if (!relation) return Effect.fail(new RelationNotFound(xlogData.relationId))

                                const change = toChange(relation, xlogData)
                                if (!change) return Effect.succeed([state, []])

                                state.currentTransaction.changes.push(change)
                                return Effect.succeed([state, []])
                            case "Commit":
                                const xid = state.currentTransaction.xid
                                const changes = state.currentTransaction.changes
                                if (changes.length === 0) return Effect.succeed([state, []])

                                return Effect.succeed([state, [{
                                    xid,
                                    commitLSN: PgReplicator.formatLSN(xlogData.commitLSN),
                                    changes,
                                    acknowledge: pgReplicator.ack(xlogData.endLSN)
                                }]])

                            default:
                                return Effect.succeed([state, []])
                        }
                }
            }

        )

    )

    return {
        transaction,
        changes: transaction.pipe(
            Stream.flatMap((tx) => Stream.fromIterable(tx.changes).pipe(
                Stream.concat(Stream.fromEffect(tx.acknowledge).pipe(Stream.drain))
            ))
        )
    }
})

export const layer = (config: Config) => Layer.effect(PostgresCDC, make(config))