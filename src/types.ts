import { Data, Effect } from "effect";
import { PgReplError } from "pg-replicator";

export type Before = {
    kind: "full" | "key";
    rows: Record<string, unknown>;
}

export type CDCChange = Data.TaggedEnum<{
    Insert: {
        schema: string;
        table: string;
        after: Record<string, unknown>;
    }
    Update: {
        schema: string;
        table: string;
        before?: Before;
        after: Record<string, unknown>;
    }
    Delete: {
        schema: string;
        table: string;
        before?: Before;
    }
}>

export interface CDCTransaction {
    xid: number;
    commitLSN: string;
    changes: CDCChange[]
    acknowledge: Effect.Effect<void, PgReplError>
}

export const CDCChange = Data.taggedEnum<CDCChange>()