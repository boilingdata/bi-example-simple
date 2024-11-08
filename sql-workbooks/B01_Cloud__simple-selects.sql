-- ----------------------------------------------------------------------
-- Highlight query and press Ctrl/Cmd + Enter to run query.
-- NOTE: You need to have monitoring database selected:
--       Data --> Cloud on left side menu
-- ----------------------------------------------------------------------
-- warming up: Load demo.parquet 28m rows fully into distributed AWS Lambda Functions memory in DuckDB table)
SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') LIMIT 20;
-- Group & Aggregate some trips
SELECT passenger_count, vendorid, COUNT(*) AS total_trips, MIN(fare_amount), MAX(trip_distance) FROM parquet_scan('s3://boilingdata-demo/demo.parquet') GROUP BY passenger_count, vendorid ORDER BY total_trips DESC, passenger_count, vendorid;
-- See row with largest tip
SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') ORDER BY tip_amount DESC LIMIT 10;
-- Find duration of some random trips
SELECT (tpep_dropoff_datetime::TIMESTAMP - tpep_pickup_datetime::TIMESTAMP)  AS duration FROM parquet_scan('s3://boilingdata-demo/demo.parquet') AS rawData ORDER BY RANDOM() LIMIT 50;
-- Run query over data set (taxi_locations) shared to you (demo@boilingdata.com) by dforsber@gmail.com
SELECT * FROM share('dforsber@gmail.com:taxi_locations');
-- Run query over shared data set (taxi_locations_limited) that matches to same source data set as taxi_locations (OBT example)
SELECT * FROM share('dforsber@gmail.com:taxi_locations_limited');
