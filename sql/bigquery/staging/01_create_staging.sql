-- Create the BigQuery staging dataset
CREATE SCHEMA IF NOT EXISTS `crypto_staging`
OPTIONS (
    location = 'US',
    description = 'Raw cryptocurrency data staging area'
);

-- Create the staging table
CREATE TABLE IF NOT EXISTS
    `crypto_staging.market_observations_raw`
(
    ingestion_id INT64 NOT NULL,
    source_batch_id INT64 NOT NULL,
    coin_id STRING NOT NULL,
    source_updated_at TIMESTAMP NOT NULL,
    raw_payload JSON NOT NULL,
    source_ingested_at TIMESTAMP NOT NULL,
    transfer_batch_id STRING NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(source_ingested_at)
CLUSTER BY coin_id
OPTIONS (
    description = 'Raw cryptocurrency observations transferred from PostgreSQL'
);


-- Verify the table
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM `crypto_staging.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'market_observations_raw'
ORDER BY ordinal_position;


SELECT COUNT(*) AS total_records
FROM `crypto_staging.market_observations_raw`;