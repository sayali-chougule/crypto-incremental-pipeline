
-- ==========================================
-- COINPAPRIKA SOURCE DATABASE
-- ==========================================

CREATE SCHEMA IF NOT EXISTS source;

-- 1. Track API ingestion batches
CREATE TABLE IF NOT EXISTS source.ingestion_batches (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    records_inserted INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,

    CONSTRAINT chk_ingestion_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

-- 2. Store raw market observations
CREATE TABLE IF NOT EXISTS source.market_observations (
    ingestion_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    coin_id TEXT NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_observation_batch
        FOREIGN KEY (batch_id)
        REFERENCES source.ingestion_batches(batch_id),

    CONSTRAINT uq_coin_observation
        UNIQUE (coin_id, source_updated_at)
);

-- 3. Track downstream extraction progress
CREATE TABLE IF NOT EXISTS source.pipeline_watermarks (
    pipeline_name TEXT PRIMARY KEY,
    last_ingestion_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_watermark_nonnegative
        CHECK (last_ingestion_id >= 0)
);

-- Index for incremental extraction
CREATE INDEX IF NOT EXISTS idx_observation_ingestion
    ON source.market_observations(ingestion_id);

-- Index for coin history queries
CREATE INDEX IF NOT EXISTS idx_observation_coin_time
    ON source.market_observations(
        coin_id,
        source_updated_at DESC
    );
