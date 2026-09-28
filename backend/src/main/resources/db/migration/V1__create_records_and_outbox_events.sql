-- gen_random_uuid() has been a built-in PostgreSQL function (no extension
-- needed) since Postgres 13; we're on 16.

CREATE TABLE records (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_hash VARCHAR(66) NOT NULL,
    status        VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    tx_hash       VARCHAR(66),
    block_number  BIGINT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_records_document_hash ON records (document_hash);

CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    record_id    UUID NOT NULL REFERENCES records (id),
    event_type   VARCHAR(50) NOT NULL,
    payload      JSONB NOT NULL,
    published    BOOLEAN NOT NULL DEFAULT false,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

-- The outbox relay's only query is "find unpublished rows, oldest first";
-- a partial index keeps that fast regardless of how large the published
-- backlog grows.
CREATE INDEX idx_outbox_events_unpublished ON outbox_events (created_at)
    WHERE published = false;
