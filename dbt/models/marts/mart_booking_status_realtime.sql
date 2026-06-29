select
    booking_status,
    payment_status,
    count(*) as booking_count,
    max(updated_at) as latest_update_time,
    sum(ticket_price) as gross_ticket_value
from {{ ref('fact_booking') }}
group by 1, 2
