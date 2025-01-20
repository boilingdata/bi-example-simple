INSTALL avro FROM community;
LOAD avro;

-- 0. Figure out all Iceberg Table versions (through all manifest files)
SET VARIABLE num_of_versions = ( SELECT "last-sequence-number" AS version FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1) ORDER BY "last-sequence-number" DESC LIMIT 1 );
SELECT getvariable('num_of_versions');

SET VARIABLE metadata_json_path_latest = ( SELECT "last-sequence-number" AS version, filename FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1) ORDER BY "last-sequence-number" DESC LIMIT 1 );
SET VARIABLE metadata_json_path_previous = ( SELECT "last-sequence-number" AS version, filename FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1) ORDER BY "last-sequence-number" DESC OFFSET 1 LIMIT 1 );
-- 1. Pick the latest Iceberg Table version
SET VARIABLE manifest_list_avro_file = (SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json(getvariable('metadata_json_path')));
-- 2. snapshot manifest-list Avro file
SET VARIABLE manifests = ( SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list_avro_file')));
-- 3. the actual manifest Avro file
SET VARIABLE data_files = ( SELECT list(data_file['file_path']) AS path FROM read_avro(getvariable('manifests')) );
-- 4. read the table data (latest version 2)
SELECT COUNT(*) FROM parquet_scan(getvariable('data_files'));
