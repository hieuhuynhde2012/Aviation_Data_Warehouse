select
    route,
    origin_airport,
    destination_airport,
    avg(distance_miles) as avg_distance_miles,
    count(*) as observed_flight_count
from {{ ref('stg_flights') }}
group by 1, 2, 3
