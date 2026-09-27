-- Example schema for examples/demo.ts.
-- Requires `wal_level = logical` in postgresql.conf (restart after changing).

CREATE TABLE todos (
    id         SERIAL PRIMARY KEY,
    title      TEXT NOT NULL,
    completed  BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Send full old-row values on UPDATE/DELETE so `before` contains every
-- column instead of just the primary key.
ALTER TABLE todos REPLICA IDENTITY FULL;

CREATE PUBLICATION my_pub FOR TABLE todos;

INSERT INTO todos (title, completed) VALUES
    ('Build replication consumer', false),
    ('Decode pgoutput messages', false),
    ('Ship the sync engine', false);
