import { Data, Effect } from "effect";
import { PgReplError } from "pg-replicator";

export type CDCChange = Data.TaggedEnum<{
    Insert: {
        schema: string;
        table: string;
        after: Record<string, unknown>;
    }
    Update: {
        schema: string;
        table: string;
        before?: Record<string, unknown>;
        after: Record<string, unknown>;
    }
    Delete: {
        schema: string;
        table: string;
        before?: Record<string, unknown>;
    }
}>

export interface CDCTransaction {
    xid: number;
    // beginLSN: bigint;
    commitLSN: bigint;
    changes: CDCChange[]
    acknowledge: Effect.Effect<void, PgReplError>
}

export const CDCChange = Data.taggedEnum<CDCChange>()