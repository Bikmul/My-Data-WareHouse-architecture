drop table car_raw_json_data_local ON CLUSTER cluster_2shards_2replicas;

CREATE TABLE car_raw_json_data_local ON CLUSTER cluster_2shards_2replicas
(
    source String,
    query_dttm DateTime DEFAULT now(),
    raw_json String,
    condition String,
    body_id String,
    model String,
    brand String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/car_raw_json_data_local', '{replica}')
PARTITION BY toYYYYMM(query_dttm)
ORDER BY (query_dttm, source)
SETTINGS index_granularity = 8192;