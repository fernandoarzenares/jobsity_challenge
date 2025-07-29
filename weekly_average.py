import pandas as pd
from sqlalchemy import create_engine
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

# Read data from trips table
df = pd.read_sql("SELECT * FROM trips", engine)

# Ensure datetime column is in datetime format
df["datetime"] = pd.to_datetime(df["datetime"])

# Extract year and week number
df["year"] = df["datetime"].dt.isocalendar().year
df["week"] = df["datetime"].dt.isocalendar().week

# Option 1: Filter by region
REGION_FILTER = "Turin"  # <-- Change this as needed

df_region = df[df["region"] == REGION_FILTER]

# Option 2: Filter by bounding box (latitude and longitude)
# BOUNDING BOX: min_lat, max_lat, min_lon, max_lon
MIN_LAT = 44.9
MAX_LAT = 45.2
MIN_LON = 7.5
MAX_LON = 7.8

df_bbox = df[
    (df["origin_coord"].str.contains("POINT")) &
    (df["origin_coord"].str.extract(r"\(([\d\.\-]+) ([\d\.\-]+)\)").astype(float)[0].between(MIN_LON, MAX_LON)) &
    (df["origin_coord"].str.extract(r"\(([\d\.\-]+) ([\d\.\-]+)\)").astype(float)[1].between(MIN_LAT, MAX_LAT))
]

# Choose which dataframe to use
df_filtered = df_region  # or df_bbox

# Group by year and week, then count trips
weekly_counts = df_filtered.groupby(["year", "week"]).size().reset_index(name="trip_count")

# Calculate average
average_weekly_trips = weekly_counts["trip_count"].mean()

print("Weekly average trips:", round(average_weekly_trips, 2))
