FROM 's3://athena-results-dforsber/iceberg_table/data/7bg-Vw/20240430_092807_00132_aasp8-31d887ac-5752-4dae-bcc6-eb9a9eef39d4.parquet' LIMIT 10;

FROM read_json_auto('s3://athena-results-dforsber/iceberg_table/metadata/*.json');

FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/snap-*.avro')

SELECT * FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');
SELECT data_file['file_path'] FROM read_avro('s3://athena-results-dforsber/iceberg_table/metadata/ffe5b608-5643-4c41-a25d-e1c686d50a05-m0.avro');