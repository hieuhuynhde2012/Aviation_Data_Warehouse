select *
from {{ ref('fact_flight_status') }}
where delay_minutes < 0
