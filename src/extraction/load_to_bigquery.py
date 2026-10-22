
import os
import uuid

from dotenv import load_dotenv
from google.cloud import bigquery

from extract_incremental import extract_incremental, get_connection

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
DATASET = os.getenv("BIGQUERY_STAGING_DATASET", "crypto_staging")

if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID is missing from .env")

DELTA_TABLE = f"{PROJECT_ID}.{DATASET}.market_observations_delta"
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.market_observations_raw"

PIPELINE_NAME = "postgres_to_bigquery"


def load_delta(client, records, transfer_batch_id):
    rows = []

    for record in records:
        rows.append({
            **record,
            "transfer_batch_id": transfer_batch_id
        })

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job = client.load_table_from_json(
        rows,
        DELTA_TABLE,
        job_config=job_config,
        location=LOCATION
    )

    job.result()

    print(f"Loaded {len(rows)} records into delta table.")



def merge_into_staging(client):
    sql = f"""
    MERGE `{RAW_TABLE}` AS target
    USING (
        SELECT *
        FROM `{DELTA_TABLE}`
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ingestion_id
            ORDER BY source_ingested_at DESC
        ) = 1
    ) AS source
    ON target.ingestion_id = source.ingestion_id

    WHEN NOT MATCHED THEN
        INSERT (
            ingestion_id,
            source_batch_id,
            coin_id,
            source_updated_at,
            raw_payload,
            source_ingested_at,
            transfer_batch_id
        )
        VALUES (
            source.ingestion_id,
            source.source_batch_id,
            source.coin_id,
            source.source_updated_at,
            TO_JSON(source.raw_payload),
            source.source_ingested_at,
            source.transfer_batch_id
        )
    """

    client.query(sql, location=LOCATION).result()

    print("MERGE completed successfully.")


def verify_staging(client, records):
    ids = [r["ingestion_id"] for r in records]

    sql = f"""
        SELECT ingestion_id, COUNT(*) AS row_count
        FROM `{RAW_TABLE}`
        WHERE ingestion_id IN UNNEST(@ids)
        GROUP BY ingestion_id
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ids", "INT64", ids)
        ]
    )

    result = list(
        client.query(
            sql,
            job_config=config,
            location=LOCATION
        ).result()
    )

    counts = {row.ingestion_id: row.row_count for row in result}

    if len(counts) != len(ids):
        raise RuntimeError("Some extracted records are missing in BigQuery")

    if any(counts.get(row_id) != 1 for row_id in ids):
        raise RuntimeError("Missing or duplicate staging records detected")

    print("BigQuery staging verification passed.")


def update_watermark(last_ingestion_id):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE source.pipeline_watermarks
                SET last_ingestion_id = %s,
                    updated_at = NOW()
                WHERE pipeline_name = %s
                  AND last_ingestion_id <= %s
            """, (
                last_ingestion_id,
                PIPELINE_NAME,
                last_ingestion_id
            ))

            if cursor.rowcount != 1:
                raise RuntimeError("Watermark update failed")

    print(f"Watermark updated to {last_ingestion_id}")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    watermark, records = extract_incremental()

    if not records:
        print("No new records to transfer.")
        return

    transfer_batch_id = str(uuid.uuid4())
    last_id = max(r["ingestion_id"] for r in records)

    print(f"Transfer batch: {transfer_batch_id}")
    print(f"Extracted records: {len(records)}")
    print(f"Previous watermark: {watermark}")

    # 1. Load the batch into BigQuery delta.
    load_delta(client, records, transfer_batch_id)

    # 2. Merge into the permanent staging table.
    merge_into_staging(client)

    # 3. Confirm each source ID exists exactly once.
    verify_staging(client, records)

    # 4. Advance the checkpoint only after verification.
    update_watermark(last_id)

    print("Incremental transfer completed successfully.")


if __name__ == "__main__":
    main()