select
    airport_code,
    airport_name,
    iso_country,
    iso_region,
    sum(origin_volume) as origin_volume,
    sum(destination_volume) as destination_volume,
    sum(delay_count) as delay_count,
    sum(cancelled_count) as cancelled_count
from (
    select origin_airport as airport_code, count(*) as origin_volume, 0 as destination_volume,
           sum(case when is_delayed then 1 else 0 end) as delay_count,
           sum(case when is_cancelled then 1 else 0 end) as cancelled_count
    from {{ ref('fact_flight_status') }}
    group by 1
    union all
    select destination_airport as airport_code, 0 as origin_volume, count(*) as destination_volume,
           sum(case when is_delayed then 1 else 0 end) as delay_count,
           sum(case when is_cancelled then 1 else 0 end) as cancelled_count
    from {{ ref('fact_flight_status') }}
    group by 1
) a
left join {{ ref('dim_airport') }} d using (airport_code)
group by 1, 2, 3, 4
