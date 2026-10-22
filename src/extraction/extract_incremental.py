
import os
import json
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

PIPELINE_NAME = "postgres_to_bigquery"
BATCH_SIZE = 100


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_source"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


def extract_incremental():
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:

            # Step 1: Read the last successful checkpoint.
            cursor.execute("""
                SELECT last_ingestion_id
                FROM source.pipeline_watermarks
                WHERE pipeline_name = %s
            """, (PIPELINE_NAME,))

            result = cursor.fetchone()

            if result is None:
                raise ValueError(
                    f"Watermark not found for {PIPELINE_NAME}"
                )

            watermark = result["last_ingestion_id"]

            print(f"Current watermark: {watermark}")

            # Step 2: Extract only new records.
            cursor.execute("""
                SELECT
                    ingestion_id,
                    batch_id,
                    coin_id,
                    source_updated_at,
                    raw_payload,
                    ingested_at
                FROM source.market_observations
                WHERE ingestion_id > %s
                ORDER BY ingestion_id
                LIMIT %s
            """, (watermark, BATCH_SIZE))

            records = cursor.fetchall()

            print(f"Records extracted: {len(records)}")

            # Step 3: Convert records to JSON-compatible data.
            extracted_records = []

            for record in records:
                extracted_records.append({
                    "ingestion_id": record["ingestion_id"],
                    "source_batch_id": record["batch_id"],
                    "coin_id": record["coin_id"],
                    "source_updated_at": (
                        record["source_updated_at"].isoformat()
                    ),
                    "raw_payload": record["raw_payload"],
                    "source_ingested_at": (
                        record["ingested_at"].isoformat()
                    )
                })

            return watermark, extracted_records


if __name__ == "__main__":
    watermark, records = extract_incremental()

    print("\nExtracted observations:")

    for record in records:
        print(
            f"ID: {record['ingestion_id']} | "
            f"Coin: {record['coin_id']} | "
            f"Source timestamp: {record['source_updated_at']}"
        )

    if records:
        print(
            "\nHighest extracted ingestion ID:",
            max(r["ingestion_id"] for r in records)
        )
