CREATE TABLE car_first_appearance_local ON CLUSTER cluster_2shards_2replicas (
    offer_url String,
    brand String,
    model String,
    body_id String,
    product_name String,
    first_seen_date DateTime,
    last_seen_date DateTime,
    days_since_first UInt32,
    current_price UInt64,
    current_availability String,
    current_creator_name String,
    is_active UInt8 DEFAULT 1,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/car_first_appearance_local', '{replica}')
ORDER BY (brand, model, offer_url)
PARTITION BY toYYYYMM(first_seen_date)
COMMENT 'Таблица первых появлений автомобилей';