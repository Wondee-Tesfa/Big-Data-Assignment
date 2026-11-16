import os
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
# PROCESSED_DATA_DIR = DATA_DIR / "processed"
# REJECTED_DATA_DIR = DATA_DIR / "rejected"
SQL_DIR = PROJECT_ROOT / "sql"
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOCS_DIR = PROJECT_ROOT / "docs"

# Source File Name
CSV_FILENAME = "yellow_tripdata_2015-01.csv"
DEFAULT_CSV_PATH = RAW_DATA_DIR / CSV_FILENAME

# Ingestion Settings (reads CHUNK_SIZE env var if set, defaults to 500,000)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500000"))

# Database Configuration (reads environment variables with local defaults)
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "taxi_db"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "1234"),
}

# Date & Geographical Boundaries
DT_FORMAT = "%Y-%m-%d %H:%M:%S"
PERIOD_START = "2015-01-01"
PERIOD_END = "2015-02-01"

# NYC Coordinate Bounding Box
LON_MIN, LON_MAX = -74.30, -73.65
LAT_MIN, LAT_MAX = 40.45, 40.95

# Permitted Master Code Sets
VALID_VENDORS = {1, 2}
VALID_RATE_CODES = {1, 2, 3, 4, 5, 6, 99}
VALID_PAYMENT_TYPES = {1, 2, 3, 4, 5, 6}

# Column Specifications
NUMERIC_COLS = [
    "VendorID", "passenger_count", "trip_distance",
    "pickup_longitude", "pickup_latitude",
    "RateCodeID", "dropoff_longitude", "dropoff_latitude",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]

MONEY_COLS = [
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
]

RENAME_MAP = {
    "VendorID": "vendor_id",
    "RateCodeID": "rate_code_id",
    "payment_type": "payment_type_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
}

# Column Ordering strictly matching PostgreSQL COPY target
TRIP_COLUMNS = [
    "vendor_id", "rate_code_id", "payment_type_id",
    "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance",
    "pickup_longitude", "pickup_latitude", "dropoff_longitude",
    "dropoff_latitude", "store_and_fwd_flag",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount",
    "trip_duration_min", "pickup_hour", "pickup_dow",
]
