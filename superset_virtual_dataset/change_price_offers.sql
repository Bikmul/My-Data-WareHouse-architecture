WITH 
    ordered_prices AS (
        SELECT 
            offer_url,
            image_url,
            brand,
            model,
            body_id,
            product_name,
            query_dttm,
            offer_price,
            creator_name,
            lagInFrame(offer_price) OVER (
                PARTITION BY offer_url 
                ORDER BY query_dttm
                ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
            ) as prev_price
        FROM car_parse
    )
SELECT 
    offer_url,
    image_url,
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
    creator_name,
    CASE 
        WHEN prev_price > 0 AND offer_price < prev_price THEN 1 
        ELSE 0 
    END as is_price_drop,
    now() as updated_at
FROM ordered_prices
WHERE prev_price IS NOT NULL
  and query_dttm >= (select max(query_dttm) - interval 10 second from car_parse);