import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

connection = None

try:
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_source"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT current_database(), current_user;
        """)

        database, user = cursor.fetchone()

        print("Connection successful!")
        print("Database:", database)
        print("User:", user)

except psycopg2.Error as error:
    print("PostgreSQL connection failed:")
    print(error)

finally:
    if connection is not None:
        connection.close()