create materialized view mv_car_stat_parser
to car_stat_parse as
select 
query_dttm,
JSONExtractString(raw_json, 'offers', 'offerCount') as offerCount,
JSONExtractString(raw_json, 'offers', 'lowPrice') as min_price,
JSONExtractString(raw_json, 'offers', 'highPrice') as max_price
from car_raw_json_data
where query_dttm = (select max(query_dttm)	from car_raw_json_data)