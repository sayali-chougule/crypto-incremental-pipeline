
import os
import json
import time
import random
import requests
import psycopg2

from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()

BASE_URL = "https://api.coinpaprika.com/v1"

COINS = [
    "btc-bitcoin",
    "eth-ethereum"
]


# -----------------------------------------
# 1. PostgreSQL connection
# -----------------------------------------
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_source"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )


# -----------------------------------------
# 2. Fetch data from CoinPaprika
# -----------------------------------------
def fetch_coin(coin_id, max_attempts=3):
    url = f"{BASE_URL}/tickers/{coin_id}"

    for attempt in range(max_attempts):
        try:
            response = requests.get(
                url,
                params={"quotes": "USD"},
                timeout=30
            )

            response.raise_for_status()
            data = response.json()

            # Validate required fields
            if data.get("id") != coin_id:
                raise ValueError(
                    f"Unexpected coin ID for {coin_id}"
                )

            if not data.get("last_updated"):
                raise ValueError(
                    f"Missing last_updated for {coin_id}"
                )

            usd = data.get("quotes", {}).get("USD", {})

            if usd.get("price") is None:
                raise ValueError(
                    f"Missing USD price for {coin_id}"
                )

            # Validate timestamp format
            datetime.fromisoformat(
                data["last_updated"].replace("Z", "+00:00")
            )

            return data

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError
        ) as error:
            if attempt == max_attempts - 1:
                raise

        except requests.exceptions.HTTPError as error:
            status = error.response.status_code

            # Retry rate limits and server errors
            if status != 429 and status < 500:
                raise

            if attempt == max_attempts - 1:
                raise

        # Exponential backoff with jitter
        delay = (2 ** attempt) + random.uniform(0, 1)
        print(f"Retrying {coin_id} in {delay:.1f}s")
        time.sleep(delay)

    raise RuntimeError(f"Failed to fetch {coin_id}")


# -----------------------------------------
# 3. Create ingestion batch
# -----------------------------------------
def create_batch():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO source.ingestion_batches
                    (status)
                VALUES ('RUNNING')
                RETURNING batch_id;
            """)

            return cursor.fetchone()[0]


# -----------------------------------------
# 4. Save observations atomically
# -----------------------------------------
def save_observations(batch_id, observations):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            inserted = 0

            for data in observations:
                cursor.execute("""
                    INSERT INTO source.market_observations (
                        batch_id,
                        coin_id,
                        source_updated_at,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (
                        coin_id,
                        source_updated_at
                    )
                    DO NOTHING;
                """, (
                    batch_id,
                    data["id"],
                    data["last_updated"],
                    Json(data)
                ))

                inserted += cursor.rowcount

            cursor.execute("""
                UPDATE source.ingestion_batches
                SET status = 'SUCCESS',
                    completed_at = NOW(),
                    records_inserted = %s
                WHERE batch_id = %s;
            """, (inserted, batch_id))

            # Connection context commits all changes
            # together when this function succeeds.
            return inserted


# -----------------------------------------
# 5. Record failed ingestion
# -----------------------------------------
def mark_batch_failed(batch_id, error):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE source.ingestion_batches
                SET status = 'FAILED',
                    completed_at = NOW(),
                    error_message = %s
                WHERE batch_id = %s;
            """, (str(error)[:2000], batch_id))


# -----------------------------------------
# 6. Main ingestion workflow
# -----------------------------------------
def main():
    batch_id = create_batch()
    print(f"Started ingestion batch: {batch_id}")

    try:
        observations = []

        for coin_id in COINS:
            print(f"Fetching {coin_id}...")

            data = fetch_coin(coin_id)
            observations.append(data)

            print(
                f"Fetched {data['symbol']}: "
                f"${data['quotes']['USD']['price']}"
            )

        inserted = save_observations(
            batch_id,
            observations
        )

        print(f"Batch {batch_id} completed successfully.")
        print(f"New observations inserted: {inserted}")

    except Exception as error:
        try:
            mark_batch_failed(batch_id, error)
        except Exception as logging_error:
            print(f"Could not log failure: {logging_error}")

        print(f"Batch {batch_id} failed: {error}")
        raise


if __name__ == "__main__":
    main()