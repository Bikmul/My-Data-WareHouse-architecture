SELECT DISTINCT
    cp.creator_name,
    length(cp.creator_name),
    cp.brand,
    cp.model,
    cp.body_id,
    COUNT(*) OVER(PARTITION BY dg.iso_code,cp.creator_name, cp.brand, cp.model, cp.body_id) as total_offers,
    COUNT(DISTINCT cp.offer_url) OVER(PARTITION BY dg.iso_code,cp.creator_name, cp.brand, cp.model, cp.body_id) as unique_cars,
    min(cp.offer_price) OVER(PARTITION BY dg.iso_code,dg.iso_code,cp.brand, cp.model, cp.body_id) as min_price,
    dg.iso_code,
    dg.city, 
    dg.lon
FROM default.car_parse cp
LEFT JOIN dealers_guide dg ON cp.creator_name = dg.dealer_name
WHERE query_dttm > (select max(query_dttm) - interval 10 second from car_parse) and model ='m3'
ORDER BY total_offers DESC;