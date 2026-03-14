drop table car_stat_parse_local ON CLUSTER cluster_2shards_2replicas;

CREATE TABLE car_stat_parse_local ON CLUSTER cluster_2shards_2replicas
(
    query_dttm DateTime DEFAULT now(),
    offerCount String,
    min_price String,
    max_price String,
    condition String,
    body_id String,
    model String,
    brand String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/car_stat_parse_local', '{replica}')
PARTITION BY toYYYYMM(query_dttm)
ORDER BY (query_dttm)
SETTINGS index_granularity = 8192;




