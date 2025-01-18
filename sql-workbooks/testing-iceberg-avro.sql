-- 0. Figure out all Iceberg Table versions through all manifest files
FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);
SELECT filename, "last-sequence-number" AS seq FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json', filename=1);

-- 1. Iceberg Table version file; it may contain multiple versions
FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json');
SET VARIABLE manifest_list = (SELECT snapshots[2]['manifest-list'] AS path FROM read_json('s3://athena-results-dforsber/iceberg_table/metadata/00001-7ee2692b-2afe-4551-b11d-ffcae35c519e.metadata.json'));
SELECT getvariable('manifest_list');

-- 2. snapshot manifest-list Avro file
FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/snap-1148784968388100414-1-ffe5b608-5643-4c41-a25d-e1c686d50a05.avro')
FROM read_avro(getvariable('manifest_list'));
SET VARIABLE manifests = ( SELECT list(manifest_path) AS path FROM read_avro(getvariable('manifest_list')));

-- 3. the actual manifest Avro file
FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro')
FROM read_avro(getvariable('manifests'));
SELECT data_file['file_path'] FROM read_avro(getvariable('manifests'));


--
--
SELECT * FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');
SELECT data_file FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');
SELECT data_file['file_path'] FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');