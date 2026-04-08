with filtered_data as (
  select * from default.car_parse
  where 1=1
    {% if filter_values('brand') %}
      and brand in ('{{ filter_values('brand') | join("','") }}')
    {% endif %}
    {% if filter_values('model') %}
      and model in ('{{ filter_values('model') | join("','") }}')
    {% endif %}
    {% if filter_values('body_id') %}
      and body_id in ('{{ filter_values('body_id') | join("','") }}')
    {% endif %}
)
select *
from (
  select *,
    rank() over(
      partition by brand, model, body_id 
      order by query_dttm desc
    ) as rn
  from filtered_data
) t
where rn = 1