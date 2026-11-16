-- =====================================================================
-- NYC Yellow Taxi Relational Database Schema
-- Architecture: Star Schema with 3NF-compliant Dimension Tables
-- =====================================================================

DROP TABLE IF EXISTS trips CASCADE;
DROP TABLE IF EXISTS vendors CASCADE;
DROP TABLE IF EXISTS rate_codes CASCADE;
DROP TABLE IF EXISTS payment_types CASCADE;

-- ---------- Dimension / Lookup Tables ----------
CREATE TABLE vendors (
    vendor_id    SMALLINT     PRIMARY KEY,
    vendor_name  VARCHAR(60)  NOT NULL
);

CREATE TABLE rate_codes (
    rate_code_id  SMALLINT     PRIMARY KEY,
    description   VARCHAR(60)  NOT NULL
);

CREATE TABLE payment_types (
    payment_type_id  SMALLINT     PRIMARY KEY,
    description      VARCHAR(60)  NOT NULL
);

-- ---------- Central Fact Table ----------
CREATE TABLE trips (
    trip_id               BIGSERIAL        PRIMARY KEY,
    vendor_id             SMALLINT         NOT NULL REFERENCES vendors (vendor_id),
    rate_code_id          SMALLINT         NOT NULL REFERENCES rate_codes (rate_code_id),
    payment_type_id       SMALLINT         NOT NULL REFERENCES payment_types (payment_type_id),

    pickup_datetime       TIMESTAMP        NOT NULL,
    dropoff_datetime      TIMESTAMP        NOT NULL,
    passenger_count       SMALLINT         NOT NULL,
    trip_distance         NUMERIC(8,2)     NOT NULL,

    pickup_longitude      DOUBLE PRECISION NOT NULL,
    pickup_latitude       DOUBLE PRECISION NOT NULL,
    dropoff_longitude     DOUBLE PRECISION NOT NULL,
    dropoff_latitude      DOUBLE PRECISION NOT NULL,
    store_and_fwd_flag    CHAR(1)          NOT NULL DEFAULT 'N',

    fare_amount           NUMERIC(8,2)     NOT NULL,
    extra                 NUMERIC(8,2)     NOT NULL,
    mta_tax               NUMERIC(8,2)     NOT NULL,
    tip_amount            NUMERIC(8,2)     NOT NULL,
    tolls_amount          NUMERIC(8,2)     NOT NULL,
    improvement_surcharge NUMERIC(8,2)     NOT NULL,
    total_amount          NUMERIC(8,2)     NOT NULL,

    -- Precomputed derived analytical columns
    trip_duration_min     NUMERIC(8,2)     NOT NULL,
    pickup_hour           SMALLINT         NOT NULL,
    pickup_dow            SMALLINT         NOT NULL,   -- 0=Sunday ... 6=Saturday

    -- Storage engine domain constraints
    CONSTRAINT chk_dropoff_after_pickup CHECK (dropoff_datetime > pickup_datetime),
    CONSTRAINT chk_passenger_count      CHECK (passenger_count BETWEEN 1 AND 6),
    CONSTRAINT chk_trip_distance        CHECK (trip_distance > 0),
    CONSTRAINT chk_fare_amount          CHECK (fare_amount >= 0),
    CONSTRAINT chk_total_amount         CHECK (total_amount > 0),
    CONSTRAINT chk_store_fwd_flag       CHECK (store_and_fwd_flag IN ('Y', 'N')),
    CONSTRAINT chk_pickup_hour          CHECK (pickup_hour BETWEEN 0 AND 23),
    CONSTRAINT chk_pickup_dow           CHECK (pickup_dow BETWEEN 0 AND 6)
);
