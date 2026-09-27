import { Data } from "effect";

/**
 * Raised when the replication connection to Postgres cannot be established.
 * `cause` holds the original error from `pg`.
 */
export class PostgresConnectionError extends Data.TaggedError("PostgresConnectionError")<{
    readonly message: string
    readonly cause?: unknown
}> { }

/**
 * Raised when a row change references a relation for which no `Relation`
 * message was received. This indicates the decoder state is out of sync
 * with the server.
 */
export class RelationNotFound extends Data.TaggedError("RelationNotFound")<{
    readonly relationId: number
}> {
    constructor(relationId: number) {
        super({ relationId });
    }
}
