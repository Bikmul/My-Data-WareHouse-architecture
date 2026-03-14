CREATE TABLE car_parse_local ON CLUSTER cluster_2shards_2replicas
(
    site String,
    query_dttm DateTime DEFAULT now(),
    brand String,
    model String,
    body_id String,
    product_name String,
    total_offers Int64,
    offer_number Int64,
    offer_url String,
    offer_price Int64,	
    availability String,
    price_currency String,
    price_valid_until String,
    image_url String,
    image_name String,
    creator_name String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/car_parse_local', '{replica}')
PARTITION BY toYYYYMM(query_dttm)
ORDER BY (query_dttm)
SETTINGS index_granularity = 8192;