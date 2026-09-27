import { Data } from "effect";

export class PostgresConnectionError extends Data.TaggedError("PostgresConnectionError")<{ message: string }> {
    constructor(message: string) {
        super({ message });
    }
}