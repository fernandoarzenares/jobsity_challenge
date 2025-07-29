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

# Read data from the "trips" table
df = pd.read_sql("SELECT * FROM trips", engine)

# Ensure the datetime column is in proper format
df['datetime'] = pd.to_datetime(df['datetime'])

# Function to classify the time of day
def classify_time_of_day(dt):
    hour = dt.hour
    if 0 <= hour < 6:
        return 'night'
    elif 6 <= hour < 12:
        return 'morning'
    elif 12 <= hour < 18:
        return 'afternoon'
    else:
        return 'evening'

# Create a new column with time of day classification
df['time_of_day'] = df['datetime'].apply(classify_time_of_day)

# Group by origin, destination, and time of day
grouped_df = df.groupby(
    ['origin_coord', 'destination_coord', 'time_of_day']
).size().reset_index(name='trip_count')

# Show sample of the result
print("Grouping completed successfully:")
print(grouped_df.head(10))

# Save result into a new table
grouped_df.to_sql("trips_grouped", engine, if_exists="replace", index=False)
print("Results saved to 'trips_grouped' table.")
