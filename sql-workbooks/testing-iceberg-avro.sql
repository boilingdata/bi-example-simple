-- 0. Figure out all Iceberg Table versions (through all manifest files)
FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);
SELECT filename, "last-sequence-number" AS seq FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);

-- 1. Pick the latest Iceberg Table version
-- s3://athena-results-dforsber/iceberg_table/metadata/00000-f9488d45-3109-43f3-b373-f2ad2afd1c45.metadata.json
FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json');
-- NOTE: With these example files, we noticed that the latest metadata.json file also contains the older metadata.json file version
SELECT snapshots[1]['snapshot-id'] FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00000-f9488d45-3109-43f3-b373-f2ad2afd1c45.metadata.json'); -- older
SELECT snapshots[1]['snapshot-id'] FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json'); -- newer, but contains the older snapshot too
-- We take the latest metadata.json file and the latest version from it.
SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json')

SET VARIABLE manifest_list_avro_file = (SELECT snapshots[len(snapshots)]['manifest-list'] AS path FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json'));
SELECT getvariable('manifest_list_avro_file') AS path;

-- 2. snapshot manifest-list Avro file
FROM read_avro(getvariable('manifest_list_avro_file'));
SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list'))
SET VARIABLE manifests = ( SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list')));

-- 3. the actual manifest Avro file
FROM read_avro(getvariable('manifests'));
SELECT data_file['file_path'] FROM read_avro(getvariable('manifests'));

-- 4. read the table data
SET VARIABLE data_files = ( SELECT list(data_file['file_path']) AS path FROM read_avro(getvariable('manifests')) );
SELECT COUNT(*) FROM parquet_scan(getvariable('data_files'));



--
--
SELECT * FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');
SELECT data_file FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');
SELECT data_file['file_path'] FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');