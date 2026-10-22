CREATE TABLE IF NOT EXISTS
    `crypto_staging.market_observations_delta`
(
    ingestion_id INT64 NOT NULL,
    source_batch_id INT64 NOT NULL,
    coin_id STRING NOT NULL,
    source_updated_at TIMESTAMP NOT NULL,
    raw_payload JSON NOT NULL,
    source_ingested_at TIMESTAMP NOT NULL,
    transfer_batch_id STRING NOT NULL
)
OPTIONS (
    description = 'Temporary delta records for incremental transfer'
);