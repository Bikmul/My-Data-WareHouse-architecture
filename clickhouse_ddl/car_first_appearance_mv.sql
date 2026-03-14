drop view car_first_appearance_mv  on cluster cluster_2shards_2replicas;

CREATE MATERIALIZED VIEW car_first_appearance_mv  on cluster cluster_2shards_2replicas
TO car_first_appearance_local
AS 
WITH 
    current_data AS (
        SELECT 
            offer_url,
            brand,
            model,
            body_id,
            product_name,
            argMax(query_dttm, query_dttm) as last_seen_date,
            argMax(offer_price, query_dttm) as current_price,
            argMax(availability, query_dttm) as current_availability,
            argMax(creator_name, query_dttm) as current_creator_name
        FROM car_parse_local
        GROUP BY offer_url, brand, model, body_id, product_name
    ),
    first_seen AS (
        SELECT 
            offer_url,
            MIN(query_dttm) as first_date
        FROM car_parse_local
        GROUP BY offer_url
    )
SELECT 
    c.offer_url, 
    c.brand,
    c.model,
    c.body_id,
    c.product_name,
    f.first_date as first_seen_date,
    c.last_seen_date,
    dateDiff('day', f.first_date, c.last_seen_date) as days_since_first,
    c.current_price,
    c.current_availability,
    c.current_creator_name,
    1 as is_active,
    now() as updated_at
FROM current_data c
JOIN first_seen f ON c.offer_url = f.offer_url;







