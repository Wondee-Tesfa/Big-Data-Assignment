import logging
from collections import Counter
import pandas as pd
from src.config import (
    DT_FORMAT, PERIOD_START, PERIOD_END,
    LON_MIN, LON_MAX, LAT_MIN, LAT_MAX,
    VALID_VENDORS, VALID_RATE_CODES, VALID_PAYMENT_TYPES,
    NUMERIC_COLS, MONEY_COLS, RENAME_MAP,
)

log = logging.getLogger("etl.transform")


def clean_chunk(raw: pd.DataFrame) -> tuple[pd.DataFrame, Counter]:
    """
    Cleans and transforms a single DataFrame chunk using vectorized operations,
    discarding rejected/dropped rows without writing them to disk.
    """
    stats: Counter = Counter()
    stats["raw_rows"] = len(raw)
    df = raw.copy()

    # 1. Type Coercion -------------------------------------------------
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], format=DT_FORMAT, errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], format=DT_FORMAT, errors="coerce"
    )
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 2. String Standardization ----------------------------------------
    flag = df["store_and_fwd_flag"].astype(str).str.strip().str.upper()
    df["store_and_fwd_flag"] = flag.where(flag.isin(["Y", "N"]), "N")

    # 3. Missing Value Filtering ---------------------------------------
    required = ["tpep_pickup_datetime", "tpep_dropoff_datetime"] + NUMERIC_COLS
    before = len(df)
    df = df.dropna(subset=required)
    stats["null_values"] = before - len(df)

    # 4. In-Chunk Deduplication ----------------------------------------
    before = len(df)
    df = df.drop_duplicates()
    stats["duplicates"] = before - len(df)

    # 5. Feature Engineering: Derived Trip Duration --------------------
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)

    # 6. Domain Business Validation & Outlier Removal ------------------
    period_start = pd.Timestamp(PERIOD_START)
    period_end = pd.Timestamp(PERIOD_END)

    rules = [
        ("pickup_outside_jan_2015", lambda d: d["tpep_pickup_datetime"].ge(period_start)
                                              & d["tpep_pickup_datetime"].lt(period_end)),
        ("bad_duration", lambda d: d["trip_duration_min"].gt(0)
                                   & d["trip_duration_min"].le(24 * 60)),
        ("bad_passenger_count", lambda d: d["passenger_count"].between(1, 6)),
        ("bad_trip_distance", lambda d: d["trip_distance"].gt(0)
                                        & d["trip_distance"].le(100)),
        ("bad_amounts", lambda d: d["fare_amount"].ge(0) & d["fare_amount"].le(1000)
                                  & d["total_amount"].gt(0) & d["total_amount"].le(1500)
                                  & d["tip_amount"].ge(0)),
        ("bad_coordinates", lambda d: d["pickup_longitude"].between(LON_MIN, LON_MAX)
                                      & d["pickup_latitude"].between(LAT_MIN, LAT_MAX)
                                      & d["dropoff_longitude"].between(LON_MIN, LON_MAX)
                                      & d["dropoff_latitude"].between(LAT_MIN, LAT_MAX)),
        ("bad_vendor", lambda d: d["VendorID"].isin(VALID_VENDORS)),
        ("bad_rate_code", lambda d: d["RateCodeID"].isin(VALID_RATE_CODES)),
        ("bad_payment_type", lambda d: d["payment_type"].isin(VALID_PAYMENT_TYPES)),
    ]

    for name, rule in rules:
        before = len(df)
        df = df[rule(df)]
        stats[name] = before - len(df)

    # 7. Final Types & Temporal Features -------------------------------
    for col in ["VendorID", "RateCodeID", "payment_type", "passenger_count"]:
        df[col] = df[col].astype("int64")

    df[MONEY_COLS + ["trip_distance"]] = df[MONEY_COLS + ["trip_distance"]].round(2)
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    
    # Map pandas (Monday=0..Sunday=6) to PostgreSQL (Sunday=0..Saturday=6)
    df["pickup_dow"] = (df["tpep_pickup_datetime"].dt.dayofweek + 1) % 7
    df = df.rename(columns=RENAME_MAP)

    stats["clean_rows"] = len(df)
    return df, stats