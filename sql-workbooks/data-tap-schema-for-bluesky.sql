COPY (
   SELECT CAST(filename[17: 27] AS INT32) AS __bd_ts,
          * EXCLUDE (filename)
     FROM read_json (
          '/tmp/json_files/*.json',
          ignore_errors = true,
          auto_detect = true,
          format = newline_delimited,
          filename = true,
          maximum_depth = 1,
          union_by_name = true,
          columns = {
          did: 'VARCHAR',
          time_us: 'BIGINT',
          kind: 'VARCHAR',
          commit: 'STRUCT(rev VARCHAR, operation VARCHAR, collection VARCHAR, rkey VARCHAR, record MAP(VARCHAR, JSON), cid VARCHAR)',
          account: 'STRUCT(active BOOLEAN, did VARCHAR, seq BIGINT, "time" VARCHAR, status VARCHAR)',
          identity: 'STRUCT(did VARCHAR, handle VARCHAR, seq BIGINT, "time" VARCHAR)'
          }
          )
) TO '%s'
     WITH (FORMAT 'Parquet', COMPRESSION 'ZSTD')