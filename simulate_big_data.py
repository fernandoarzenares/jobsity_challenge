import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from faker import Faker
import random
from datetime import datetime, timedelta
import time
import os
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

# Faker instance
fake = Faker()

# Trip source list
sources = ['funny_car', 'cheap_mobile', 'baba_car', 'fasttrack_app', 'bad_diesel_vehicles']

# Region list
regions = ['Turin', 'Prague', 'Berlin', 'Paris', 'Madrid']

# Number of total rows you want to generate
TOTAL_ROWS = 10_000_000  # ← Start with 10 million (adjust to 100M later if needed)
BATCH_SIZE = 1_000_000

def generate_batch(n):
    data = {
        "region": [random.choice(regions) for _ in range(n)],
        "origin_coord": [f"POINT ({round(random.uniform(7.0, 15.0), 6)} {round(random.uniform(44.0, 51.0), 6)})" for _ in range(n)],
        "destination_coord": [f"POINT ({round(random.uniform(7.0, 15.0), 6)} {round(random.uniform(44.0, 51.0), 6)})" for _ in range(n)],
        "datetime": [fake.date_time_between(start_date='-3y', end_date='now') for _ in range(n)],
        "datasource": [random.choice(sources) for _ in range(n)]
    }
    return pd.DataFrame(data)

# Insert in batches
start = time.time()
for i in range(0, TOTAL_ROWS, BATCH_SIZE):
    print(f"🚀 Inserting batch {i // BATCH_SIZE + 1}...")
    df = generate_batch(BATCH_SIZE)
    df.to_sql("trips", engine, if_exists="append", index=False, method='multi')
    print(f"Batch {i // BATCH_SIZE + 1} inserted.")

end = time.time()
print(f"All data inserted. Total time: {round(end - start, 2)} seconds.")
