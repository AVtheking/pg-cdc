# pg-cdc

Postgres change data capture (CDC) as [Effect](https://effect.website) streams.

`pg-cdc` connects to Postgres over the logical replication protocol, decodes
`pgoutput` messages, and exposes committed transactions as a typed
`Stream`. Each transaction carries an `acknowledge` effect, so you control
exactly when the replication slot advances.

> **Status:** early release. The API may change before 1.0. `effect` is
> currently a 4.0 release candidate.

## Install

```sh
pnpm add pg-cdc effect pg
```

`effect` and `pg` are peer dependencies.

## Postgres setup

Logical replication has to be enabled on the server, and you need a
publication for the tables you want to capture.

```sql
-- postgresql.conf (requires a restart)
-- wal_level = logical

CREATE PUBLICATION sync_pub FOR TABLE todos;

-- Optional: include full old-row values on UPDATE / DELETE.
-- Without this, `before` only contains the primary key.
ALTER TABLE todos REPLICA IDENTITY FULL;
```

The connecting role needs the `REPLICATION` attribute (or be a superuser).
The replication slot is created on first run if it does not exist.

## Usage

### With `make`

`make` returns a scoped `Effect`. The replication connection is closed when
the scope ends.

```ts
import { Effect, Stream } from "effect"
import { NodeRuntime } from "@effect/platform-node"
import * as PostgresCDC from "pg-cdc"

const program = Effect.gen(function* () {
  const cdc = yield* PostgresCDC.make({
    connectionString: "postgres://postgres:postgres@localhost:5432/app",
    publication: "sync_pub",
    slot: "app_slot",
  })

  yield* cdc.transaction.pipe(
    Stream.runForEach((tx) =>
      Effect.gen(function* () {
        yield* publish(tx.changes) // your side effect
        yield* tx.acknowledge      // then advance the slot
      })
    )
  )
})

program.pipe(Effect.scoped, NodeRuntime.runMain)
```

### With a `Layer`

`layer(config)` provides the `PostgresCDC` service. The connection lives as
long as the layer.

```ts
import { Effect, Stream } from "effect"
import { PostgresCDC, layer } from "pg-cdc"

const program = Effect.gen(function* () {
  const cdc = yield* PostgresCDC
  yield* cdc.transaction.pipe(Stream.runForEach(handleTransaction))
})

program.pipe(
  Effect.provide(layer({ connectionString: "...", publication: "sync_pub", slot: "app_slot" }))
)
```

Because `PostgresCDC` is a service, tests can swap in a fake:

```ts
Layer.succeed(PostgresCDC, {
  transaction: Stream.make(fakeTransaction),
  changes: Stream.make(fakeChange),
})
```

## API

### `make(config): Effect<PostgresCDCService, PostgresConnectionError | PgReplError, Scope>`

| option             | description                                              |
| ------------------ | -------------------------------------------------------- |
| `connectionString` | Postgres connection string. The role needs `REPLICATION`. |
| `publication`      | Name of an existing publication.                         |
| `slot`             | Replication slot name. Created if it does not exist.     |

### `PostgresCDCService`

```ts
interface PostgresCDCService {
  transaction: Stream<CDCTransaction, PgReplError | RelationNotFound>
  changes: Stream<CDCChange, PgReplError | RelationNotFound>
}
```

**`transaction`** — one element per committed transaction. Nothing is
acknowledged until you run `tx.acknowledge`. Use this when you need
at-least-once delivery into another system (Kafka, another database, ...).

**`changes`** — the same data flattened to individual row changes.
Acknowledgement happens automatically once all changes of a transaction have
been pulled through your consumer. Keep your side effect directly in
`Stream.runForEach` (or an equivalent sequential sink); inserting
`Stream.buffer`, `Stream.mapEffect` with `concurrency`, or a fork between the
stream and your side effect lets the acknowledgement run before your work
is done.

> Only one of `transaction` / `changes` may be consumed per `make` (or per
> layer). Both share a single replication connection and slot, and Postgres
> allows one active consumer per slot.

### `CDCTransaction`

```ts
interface CDCTransaction {
  xid: number                          // Postgres transaction id
  commitLSN: string                    // e.g. "0/19B58D8"
  changes: CDCChange[]
  acknowledge: Effect<void, PgReplError>
}
```

### `CDCChange`

A `Data.TaggedEnum` with three cases. `before` is only present when
Postgres sent old-row data (see `REPLICA IDENTITY` above).

```ts
type CDCChange =
  | { _tag: "Insert"; schema: string; table: string; after: Record<string, unknown> }
  | { _tag: "Update"; schema: string; table: string; before?: Before; after: Record<string, unknown> }
  | { _tag: "Delete"; schema: string; table: string; before?: Before }

type Before = { kind: "full" | "key"; rows: Record<string, unknown> }
```

Match on it with `CDCChange.$match` or `Match.tag`.

### Errors

| error                     | when                                                                  |
| ------------------------- | --------------------------------------------------------------------- |
| `PostgresConnectionError` | the replication connection could not be opened (`cause` has details) |
| `RelationNotFound`        | a row change referenced a table no `Relation` message was seen for    |
| `PgReplError`             | protocol / decoding error from `pg-replicator`                        |

## Delivery semantics

- **At-least-once.** If the process dies before `acknowledge`, Postgres
  redelivers the transaction on the next connection. Make your consumers
  idempotent.
- **Unacknowledged transactions retain WAL.** Postgres keeps WAL segments
  until the slot is advanced. If you never acknowledge, disk usage grows
  without bound.
- **Ordering.** Transactions are delivered in commit order.

## Development

```sh
pnpm install
pnpm typecheck
pnpm build
pnpm example   # runs examples/demo.ts against a local Postgres
```

## License

MIT
