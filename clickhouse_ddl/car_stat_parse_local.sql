CREATE TABLE car_stat_parse_local ON CLUSTER cluster_2shards_2replicas
(
    query_dttm DateTime DEFAULT now(),
    offerCount UInt8,
    min_price UInt8,
    max_price UInt8
)
ENGINE = MergeTree()  -- Простой MergeTree, не Replicated!
PARTITION BY toYYYYMM(query_dttm)
ORDER BY (query_dttm)
SETTINGS index_granularity = 8192;

select * from car_stat_parse_local;

