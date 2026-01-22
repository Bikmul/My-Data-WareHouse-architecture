CREATE TABLE car_raw_json_data_local ON CLUSTER cluster_2shards_2replicas
(
    source String,
    query_dttm DateTime DEFAULT now(),
    raw_json String
)
ENGINE = MergeTree()  -- Простой MergeTree, не Replicated!
PARTITION BY toYYYYMM(query_dttm)
ORDER BY (query_dttm, source)
SETTINGS index_granularity = 8192;

select * from car_raw_json_data_local;

