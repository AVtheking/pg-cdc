import { Data } from "effect";

export class PostgresConnectionError extends Data.TaggedError("PostgresConnectionError")<{ message: string }> {
    constructor(message: string) {
        super({ message });
    }
}

export class RelationNotFound extends Data.TaggedError("RelationNotFound")<{ relationId: number }> {
    constructor(relationId: number) {
        super({ relationId });
    }
}