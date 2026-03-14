drop view mv_car_stat_parser on cluster cluster_2shards_2replicas;
create materialized view mv_car_stat_parser on cluster cluster_2shards_2replicas
to car_stat_parse_local as
select 
query_dttm,
JSONExtractString(raw_json, 'offers', 'offerCount') as offerCount,
JSONExtractString(raw_json, 'offers', 'lowPrice') as min_price,
JSONExtractString(raw_json, 'offers', 'highPrice') as max_price,
brand,
model,
body_id,
condition
from car_raw_json_data_local
where query_dttm = (select max(query_dttm)	from car_raw_json_data)