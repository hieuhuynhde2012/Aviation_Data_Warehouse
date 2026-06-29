select
    customer_id,
    count(*) as booking_count,
    sum(case when booking_status = 'CONFIRMED' then ticket_price else 0 end) as total_spend,
    max(booking_time) as latest_booking_time,
    min(booking_time) as first_booking_time,
    case
        when count(*) = 1 then 'NEW'
        when sum(case when booking_status = 'CONFIRMED' then ticket_price else 0 end) >= 2500 then 'HIGH_VALUE'
        else 'RETURNING'
    end as lifecycle_segment
from {{ ref('fact_booking') }}
group by 1
