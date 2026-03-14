-- `default`.car_stat_parse definition

CREATE TABLE default.car_stat_parse
(

    `query_dttm` DateTime DEFAULT now(),

    `offerCount` String,

    `min_price` String,

    `max_price` String,

    `condition` String,

    `body_id` String,

    `model` String,

    `brand` String
)
ENGINE = Distributed('cluster_2shards_2replicas',
 'default',
 'car_stat_parse_local',
 rand());