select
    flight_date,
    route,
    origin_airport,
    destination_airport,
    count(*) as flight_count,
    avg(delay_minutes) as avg_delay_minutes,
    sum(case when is_delayed then 1 else 0 end) as delayed_flight_count,
    sum(case when is_cancelled then 1 else 0 end) as cancelled_flight_count
from {{ ref('fact_flight_status') }}
group by 1, 2, 3, 4
