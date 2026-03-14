-- Создаем целевую таблицу
CREATE TABLE car_price_history ON CLUSTER cluster_2shards_2replicas  (
    offer_url String,
    brand String,
    model String,
    body_id String,
    product_name String,
    check_date DateTime,
    previous_price UInt64,
    current_price UInt64,
    price_change Int64,
    price_change_percent Float64,
    availability String,
    creator_name String,	
    is_price_drop UInt8 DEFAULT 0,
    updated_at DateTime DEFAULT now()
) ENGINE = Distributed('cluster_2shards_2replicas',
 'default',
 'car_price_history_local',
 rand());