import { Data, type Effect } from "effect";
import type { PgReplError } from "pg-replicator";

/**
 * The previous version of a row, as reported by Postgres.
 *
 * `kind` is `"full"` when the table has `REPLICA IDENTITY FULL`, in which
 * case `rows` contains every column. Otherwise it is `"key"` and `rows`
 * contains only the replica identity (usually the primary key).
 */
export type Before = {
    kind: "full" | "key";
    rows: Record<string, unknown>;
}

/**
 * A single row-level change decoded from the logical replication stream.
 */
export type CDCChange = Data.TaggedEnum<{
    Insert: {
        schema: string;
        table: string;
        after: Record<string, unknown>;
    }
    Update: {
        schema: string;
        table: string;
        /** Absent when the table's replica identity does not include old values. */
        before?: Before | undefined;
        after: Record<string, unknown>;
    }
    Delete: {
        schema: string;
        table: string;
        /** Absent when the table's replica identity does not include old values. */
        before?: Before | undefined;
    }
}>

/**
 * A committed Postgres transaction and the changes it contained.
 */
export interface CDCTransaction {
    /** Postgres transaction id. */
    xid: number;
    /** LSN of the commit record, formatted as `X/Y`. */
    commitLSN: string;
    changes: CDCChange[]
    /**
     * Confirms this transaction (and everything before it) has been processed.
     *
     * Run this after your side effects have completed. Until it runs, Postgres
     * keeps the WAL for this transaction and will redeliver it on reconnect.
     */
    acknowledge: Effect.Effect<void, PgReplError>
}

export const CDCChange = Data.taggedEnum<CDCChange>()
