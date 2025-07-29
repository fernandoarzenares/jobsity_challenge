import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB = os.getenv("POSTGRES_DB")
USER = os.getenv("POSTGRES_USER")
PWD = os.getenv("POSTGRES_PASSWORD")
HOST = os.getenv("POSTGRES_HOST")
PORT = os.getenv("POSTGRES_PORT")

# Create database engine
engine = create_engine(f"postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

# Create psycopg2 connection (needed to run raw SQL)
conn = psycopg2.connect(
    dbname=DB,
    user=USER,
    password=PWD,
    host=HOST,
    port=PORT
)
cur = conn.cursor()

# Create ingestion_status table if it doesn't exist
create_status_table_sql = """
CREATE TABLE IF NOT EXISTS ingestion_status (
    id SERIAL PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status VARCHAR(50)
);
"""
cur.execute(create_status_table_sql)
conn.commit()

# Insert a new status row with "started"
start_time = datetime.now()
cur.execute(
    "INSERT INTO ingestion_status (started_at, status) VALUES (%s, %s) RETURNING id;",
    (start_time, "in_progress")
)
status_id = cur.fetchone()[0]
conn.commit()

try:
    # Read CSV
    df = pd.read_csv("trips.csv", encoding="cp1252")

    # Insert data into trips table
    df.to_sql("trips", engine, if_exists="append", index=False)

    # Update status to success
    end_time = datetime.now()
    cur.execute(
        "UPDATE ingestion_status SET finished_at = %s, status = %s WHERE id = %s;",
        (end_time, "success", status_id)
    )
    conn.commit()

    print("Data ingestion completed successfully.")

except Exception as e:
    # Update status to failed
    end_time = datetime.now()
    cur.execute(
        "UPDATE ingestion_status SET finished_at = %s, status = %s WHERE id = %s;",
        (end_time, "failed", status_id)
    )
    conn.commit()
    print("Ingestion failed:", str(e))

# Close connections
cur.close()
conn.close()
