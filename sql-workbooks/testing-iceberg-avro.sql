INSTALL avro FROM community;
LOAD avro;

-- 0. Figure out all Iceberg Table versions (through all manifest files)
FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);
SELECT filename, "last-sequence-number" AS seq FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);
SELECT filename, "last-sequence-number" AS seq FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1) ORDER BY seq DESC LIMIT 1;
SET VARIABLE metadata_json_path = ( SELECT filename FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1) ORDER BY "last-sequence-number" DESC LIMIT 1 );
SELECT getvariable('metadata_json_path') AS metadata_json_path;

-- 1. Pick the latest Iceberg Table version
-- s3://athena-results-dforsber/iceberg_table/metadata/00000-f9488d45-3109-43f3-b373-f2ad2afd1c45.metadata.json
FROM read_json(getvariable('metadata_json_path'));
-- NOTE: With these example files, we noticed that the latest metadata.json file also contains the older metadata.json table version
SELECT snapshots[1]['snapshot-id'] FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00000-f9488d45-3109-43f3-b373-f2ad2afd1c45.metadata.json'); -- older
SELECT snapshots[1]['snapshot-id'] FROM read_json(getvariable('metadata_json_path')); -- newer, but contains the older snapshot too
-- We take the latest metadata.json file and the latest version from it.
SELECT len(snapshots) AS latest_table_version FROM read_json(getvariable('metadata_json_path'));
SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json(getvariable('metadata_json_path'));

SET VARIABLE manifest_list_avro_file = (SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json(getvariable('metadata_json_path')));
SELECT getvariable('manifest_list_avro_file') AS path;

-- 2. snapshot manifest-list Avro file
FROM read_avro(getvariable('manifest_list_avro_file'));
SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list_avro_file'));
SET VARIABLE manifests = ( SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list_avro_file')));

-- 3. the actual manifest Avro file
FROM read_avro(getvariable('manifests'));
SELECT data_file['file_path'] FROM read_avro(getvariable('manifests'));
SET VARIABLE data_files = ( SELECT list(data_file['file_path']) AS path FROM read_avro(getvariable('manifests')) );

-- 4. read the table data (latest version 2)
SELECT COUNT(*) FROM parquet_scan(getvariable('data_files'));
