
drop VIEW car_price_history_mv  on cluster cluster_2shards_2replicas;
-- Создаем материализованное представление
CREATE MATERIALIZED VIEW car_price_history_mv  on cluster cluster_2shards_2replicas
TO car_price_history_local
AS 
WITH 
    ordered_prices AS (
        SELECT 
            offer_url,
            brand,
            model,
            body_id,
            product_name,
            query_dttm,
            offer_price,
            availability,
            creator_name,
            lagInFrame(offer_price) OVER (
                PARTITION BY offer_url 
                ORDER BY query_dttm
                ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
            ) as prev_price
        FROM car_parse_local
    )
SELECT 
    offer_url,
    brand,
    model,
    body_id,
    product_name,
    query_dttm as check_date,
    prev_price as previous_price,
    offer_price as current_price,
    offer_price - prev_price as price_change,
    CASE 
        WHEN prev_price > 0 THEN 
            round((offer_price - prev_price) * 100.0 / prev_price, 2)
        ELSE 0 
    END as price_change_percent,
    availability,
    creator_name,
    CASE 
        WHEN prev_price > 0 AND offer_price < prev_price THEN 1 
        ELSE 0 
    END as is_price_drop,
    now() as updated_at
FROM ordered_prices
WHERE prev_price IS NOT NULL
  AND prev_price != offer_price;


