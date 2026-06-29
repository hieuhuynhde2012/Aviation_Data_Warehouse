select
    customer_id,
    min(booking_time) as first_booking_time,
    max(booking_time) as latest_booking_time,
    count(*) as lifetime_booking_count,
    sum(case when booking_status = 'CONFIRMED' then ticket_price else 0 end) as lifetime_confirmed_revenue,
    max(customer_segment) as latest_customer_segment
from {{ ref('stg_bookings') }}
group by 1
