select
    flight_date,
    airline,
    origin_airport,
    destination_airport,
    route,
    count(*) as flight_count,
    avg(delay_minutes) as avg_delay_minutes,
    sum(case when is_delayed then 1 else 0 end)::numeric / nullif(count(*), 0) as delayed_rate,
    sum(case when is_cancelled then 1 else 0 end) as cancelled_count,
    sum(carrier_delay) as carrier_delay_minutes,
    sum(weather_delay) as weather_delay_minutes,
    sum(nas_delay) as nas_delay_minutes,
    sum(security_delay) as security_delay_minutes,
    sum(late_aircraft_delay) as late_aircraft_delay_minutes
from {{ ref('fact_flight_status') }}
group by 1, 2, 3, 4, 5
