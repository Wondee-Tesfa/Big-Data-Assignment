-- =====================================================================
-- Indexes for query performance on 12M+ rows.
-- Run AFTER the bulk load (the ETL script does this automatically).
-- =====================================================================

CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime ON trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_payment_type    ON trips (payment_type_id);
CREATE INDEX IF NOT EXISTS idx_trips_vendor          ON trips (vendor_id);
CREATE INDEX IF NOT EXISTS idx_trips_rate_code       ON trips (rate_code_id);
CREATE INDEX IF NOT EXISTS idx_trips_hour_dow        ON trips (pickup_hour, pickup_dow);

ANALYZE trips;
