# Data Engineering Challenge – Trips Analysis

This project is a technical challenge that simulates ingestion, processing, and analysis of trip data using a SQL database and Python.

## Project Overview

This solution includes:

- Automated and on-demand data ingestion
- Grouping of trips by origin, destination, and time of day
- Weekly average trip calculation by region or coordinate bounding box
- Ingestion status tracking without polling
- Scalable pipeline tested with 10 million records (ready for 100M)
- Bonus: SQL queries for reporting and a cloud deployment sketch

---

## Project Structure

├── ingest_trips.py            # Ingests the original CSV into the database  
├── group_trips.py             # Groups trips by origin, destination, and time of day  
├── weekly_average.py          # Calculates weekly average by region or bounding box  
├── simulate_big_data.py       # Inserts millions of fake trip records (for scalability)  
├── create_tables.sql          # Creates required database tables  
├── queries.sql                # Bonus SQL queries for reporting  
├── docker-compose.yml         # Docker setup for PostgreSQL  
├── .env                       # Environment configuration  
└── README.md                  # You're here

---

## How to Run the Project Locally

### 1. Requirements

- Docker & Docker Compose (make sure Docker Desktop is installed and running)
- Python 3.10+
- (Optional) pgAdmin or DBeaver if you want to inspect the database

---

### 2. Setup

```bash
# Clone the repository
git clone https://github.com/fernandoarzenares/jobsity_challenge.git
cd jobsity_challenge

# Start PostgreSQL in Docker
docker-compose up -d
```

This will:
- Start a PostgreSQL 15 container
- Create the database `jobsity_db`
- Automatically execute `create_tables.sql` on first run

---

### 3. Python Environment Setup

```bash
# Create virtual environment
py -m venv venv
venv\Scripts\activate         # On Windows
# or
source venv/bin/activate        # On Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

---

### 4. Running the Scripts

```bash
# Ingest the original CSV
python ingest_trips.py

# Group trips by origin/destination/time of day
python group_trips.py

# Calculate weekly average (region or bounding box)
python weekly_average.py

# Simulate 10M+ rows for scalability
python simulate_big_data.py
```

---

### 5. Scalable Ingestion (Millions of Rows)

The `ingest_trips.py` script is designed to handle large CSV files efficiently.

Instead of reading the entire file into memory, it uses chunked ingestion with:

```python
pd.read_csv("trips.csv", chunksize=100_000)
```

This allows:

- Stable memory usage even with millions of rows

- Better performance by inserting batches with method='multi'

- Production-level robustness

---


---

### 6. Bonus Queries

You can run the queries manually using a SQL client (pgAdmin, DBeaver, etc.):

- **Latest datasource from top 2 regions**
- **Regions where `cheap_mobile` appeared**

Queries located in `queries.sql`.

---

### Cloud Architecture Sketch (AWS Example)

If deployed to AWS, the architecture would look like this:

- **Amazon RDS (PostgreSQL)** – stores all trip data  
- **EC2 instance** – runs Python scripts for ingestion and processing  
- **S3 bucket** – stores incoming CSV files for ingestion  
- **Lambda + API Gateway** – to expose endpoints (if REST API needed)  
- **CloudWatch** – to monitor ingestion and log events
