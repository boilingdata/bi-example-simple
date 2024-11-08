-- ----------------------------------------------------------------------
-- Highlight query and press Ctrl/Cmd + Enter to run query.
-- NOTE: You need to have monitoring database selected:
--       Data --> Cloud on left side menu
-- ----------------------------------------------------------------------
-- JOIN query - 28m row demo.parquet data set is JOINed with taxi_locations.parquet
SELECT COUNT(*) AS total, l.service_zone FROM parquet_scan('s3://boilingdata-demo/demo.parquet') d
JOIN parquet_scan('s3://boilingdata-demo/taxi_locations.parquet') l ON (d.pulocationid = l.locationid) GROUP BY l.service_zone LIMIT 10;
-- more complete query
SELECT d.passenger_count, d.vendorid, l.service_zone, COUNT(*) AS trips, MIN(d.fare_amount) AS minFare, MAX(d.trip_distance) AS maxFare FROM parquet_scan ('s3://boilingdata-demo/demo.parquet') d JOIN parquet_scan ('s3://boilingdata-demo/taxi_locations.parquet') l ON (d.pulocationid = l.locationid)       GROUP BY d.passenger_count, d.vendorid, l.service_zone;
