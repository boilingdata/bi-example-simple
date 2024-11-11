-- ----------------------------------------------------------------------
-- Highlight query and press Ctrl/Cmd + Enter to run query.
-- ----------------------------------------------------------------------

-- 
-- The database, schema, and table naming are formed like below:
-- <database>.<schema>.<table>
-- 
-- 'database' is formed from 3 parameters 
--     <sourceType>_<name>_<dbName>
--  where 
--     sourceType: dataSource | ingSource
--     name: data source name
--     dbName: actual database filename without '.duckdb' ending
SHOW ALL TABLES;

-- List all DuckDB extensions and their status on local selected DuckDB database.
-- NOTE: You can set DuckDB init SQL clauses on settings. For example add all INSTALL/LOAD extensions clauses there.
SELECT extension_name, installed, loaded, description FROM duckdb_extensions() ORDER BY installed DESC, loaded DESC;

-- You can use the shell extension to list your files
INSTALL shellfs FROM community;
LOAD shellfs;
SELECT * FROM read_csv('echo "hello\nworld"|');

-- You can install secrets to DuckDB and e.g. query S3 directly from your laptop
-- However, if you configure AWS credentials, they will be used automatically
-- when querying with DuckDB while a local database is selected.
-- NOTE: BoilingData is faster accessing data on S3
-- See all credentials available:
FROM duckdb_secrets();
FROM which_secret('s3://boilingdata-demo/taxi_locations.parquet', 's3');
SELECT * FROM parquet_scan('s3://boilingdata-demo/taxi_locations.parquet');

-- No credentials needed.
-- h3 community extension. Run them one by one.
-- NOTE! Works only with local databases (i.e. not on Boiling)
INSTALL h3 FROM community;
LOAD h3;
SELECT
    h3_latlng_to_cell(pickup_latitude, pickup_longitude, 9) AS cell_id,
    h3_cell_to_boundary_wkt(cell_id) AS boundary,
    count() AS cnt
FROM read_parquet('https://blobs.duckdb.org/data/yellow_tripdata_2010-01.parquet')
GROUP BY cell_id
HAVING cnt > 10;

-- No credentials needed.
-- NOTE: You need to create the 'source.parquet' file first.
-- Export to Excel with spatial extension
--  - https://duckdb.org/docs/extensions/spatial/overview
--  - https://duckdb.org/docs/extensions/excel
INSTALL spatial;
LOAD spatial;
COPY (SELECT * FROM 'source.parquet') TO 'target.xlsx' WITH (FORMAT GDAL, DRIVER 'XLSX');

-- No credentials needed.
-- IP addresses with inet extension
--  - https://duckdb.org/docs/extensions/inet.html
INSTALL inet;
LOAD inet;
SELECT '127.0.0.1'::INET AS ipv4, '2001:db8:3c4d::/48'::INET AS ipv6;
CREATE TABLE tbl (id INTEGER, ip INET);
INSERT INTO tbl VALUES
    (1, '192.168.0.0/16'),
    (2, '127.0.0.1'),
    (3, '8.8.8.8'),
    (4, 'fe80::/10'),
    (5, '2001:db8:3c4d:15::1a2f:1a2b');
SELECT * FROM tbl;
