-- =====================================================================
-- Verification & business-analysis queries
-- Run in pgAdmin / psql and take screenshots for the report.
-- =====================================================================

-- 1. Total rows loaded (must be >= 2,000,000)
SELECT COUNT(*) AS total_trips FROM trips;

-- 2. Lookup tables
SELECT * FROM vendors;
SELECT * FROM rate_codes;
SELECT * FROM payment_types;

-- 3. Referential integrity: must return 0 rows
SELECT COUNT(*) AS orphan_rows
FROM trips t
LEFT JOIN vendors v        ON v.vendor_id = t.vendor_id
LEFT JOIN rate_codes r     ON r.rate_code_id = t.rate_code_id
LEFT JOIN payment_types p  ON p.payment_type_id = t.payment_type_id
WHERE v.vendor_id IS NULL OR r.rate_code_id IS NULL OR p.payment_type_id IS NULL;

-- 4. Data-quality ranges
SELECT MIN(pickup_datetime) AS first_pickup,
       MAX(pickup_datetime) AS last_pickup,
       MIN(trip_distance)   AS min_dist,  MAX(trip_distance)   AS max_dist,
       MIN(fare_amount)     AS min_fare,  MAX(fare_amount)     AS max_fare,
       MIN(trip_duration_min) AS min_dur, MAX(trip_duration_min) AS max_dur
FROM trips;

-- 5. Trips and revenue by hour (peak demand)
SELECT pickup_hour,
       COUNT(*)                        AS trips,
       ROUND(SUM(total_amount), 2)     AS revenue,
       ROUND(AVG(total_amount), 2)     AS avg_total
FROM trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- 6. Trips and revenue by day of week (0 = Sunday)
SELECT pickup_dow,
       COUNT(*)                        AS trips,
       ROUND(SUM(total_amount), 2)     AS revenue
FROM trips
GROUP BY pickup_dow
ORDER BY pickup_dow;

-- 7. Payment type behaviour and tipping
SELECT p.description AS payment_type,
       COUNT(*)                          AS trips,
       ROUND(AVG(t.tip_amount), 2)       AS avg_tip,
       ROUND(100 * AVG(CASE WHEN t.fare_amount > 0
                            THEN t.tip_amount / t.fare_amount END), 1) AS avg_tip_pct
FROM trips t
JOIN payment_types p ON p.payment_type_id = t.payment_type_id
GROUP BY p.description
ORDER BY trips DESC;

-- 8. Vendor comparison
SELECT v.vendor_name,
       COUNT(*)                          AS trips,
       ROUND(AVG(t.trip_distance), 2)    AS avg_distance_mi,
       ROUND(AVG(t.trip_duration_min), 1) AS avg_duration_min,
       ROUND(AVG(t.total_amount), 2)     AS avg_total
FROM trips t
JOIN vendors v ON v.vendor_id = t.vendor_id
GROUP BY v.vendor_name;

-- 9. Rate code revenue (JFK / Newark etc.)
SELECT r.description AS rate_code,
       COUNT(*)                      AS trips,
       ROUND(AVG(t.total_amount), 2) AS avg_total
FROM trips t
JOIN rate_codes r ON r.rate_code_id = t.rate_code_id
GROUP BY r.description
ORDER BY trips DESC;

-- 10. Daily revenue trend
SELECT pickup_datetime::date AS day,
       COUNT(*)                    AS trips,
       ROUND(SUM(total_amount), 2) AS revenue
FROM trips
GROUP BY day
ORDER BY day;

-- 11. Index is used (screenshot the plan; look for "Index Scan" / "Bitmap Index Scan")
EXPLAIN ANALYZE
SELECT COUNT(*) FROM trips
WHERE pickup_datetime >= '2015-01-15' AND pickup_datetime < '2015-01-16';

-- 12. Table and index size on disk
SELECT pg_size_pretty(pg_total_relation_size('trips')) AS total_size,
       pg_size_pretty(pg_relation_size('trips'))        AS table_size,
       pg_size_pretty(pg_indexes_size('trips'))         AS indexes_size;
