drop view mv_car_parser on cluster cluster_2shards_2replicas;


create materialized view mv_car_parser on cluster cluster_2shards_2replicas
to car_parse_local as
SELECT
    source AS site,
    query_dttm,
    brand,
    model,
    body_id,
    -- Основная информация
    JSONExtractString(raw_json, 'name') AS product_name,
    COALESCE(JSONExtractInt(raw_json, 'offers', 'offerCount'), 0) AS total_offers,
    -- Номер предложения
    n.number AS offer_number,
    -- Парсим каждое предложение
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'url') AS offer_url,
    JSONExtractInt(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'price') AS offer_price,
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'availability') AS availability,
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'priceCurrency') AS price_currency,
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'priceValidUntil') AS price_valid_until,
    -- Информация об изображении
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'image', 'contentUrl') AS image_url,
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'image', 'name') AS image_name,
    JSONExtractString(JSONExtractRaw(raw_json, 'offers', 'offers', n.number), 'image', 'creator', 'name') AS creator_name
FROM car_raw_json_data_local
CROSS JOIN 
    numbers(1, 200) AS n  -- Фиксируем максимум 50 предложений
WHERE 
    source = 'auto.ru'
    AND n.number <= COALESCE(JSONExtractInt(raw_json, 'offers', 'offerCount'), 0)
    AND COALESCE(JSONExtractInt(raw_json, 'offers', 'offerCount'), 0) > 0
    and query_dttm = (select max(query_dttm) from car_raw_json_data)





