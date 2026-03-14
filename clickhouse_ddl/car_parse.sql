CREATE TABLE default.car_parse ON CLUSTER cluster_2shards_2replicas
(
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
ENGINE = Distributed('cluster_2shards_2replicas',
 'default',
 'car_parse_local',
 rand());