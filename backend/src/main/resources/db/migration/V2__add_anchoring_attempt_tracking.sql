-- Phase 5: the anchoring consumer tracks how many times it has tried a
-- record and why the most recent try failed.
ALTER TABLE records
    ADD COLUMN attempts   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT,
    -- Optimistic-lock counter (JPA @Version). The consumer and the stale
    -- ANCHORING recovery job can both write the same row; a write based on
    -- an out-of-date read fails instead of silently overwriting the other.
    ADD COLUMN version    BIGINT  NOT NULL DEFAULT 0;

-- The recovery job's only query is "ANCHORING rows not touched recently";
-- in steady state that's a handful of rows out of many.
CREATE INDEX idx_records_anchoring ON records (updated_at)
    WHERE status = 'ANCHORING';
