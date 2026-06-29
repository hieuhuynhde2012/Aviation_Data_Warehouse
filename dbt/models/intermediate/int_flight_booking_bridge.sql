select
    b.flight_id,
    b.route,
    b.flight_date,
    count(*) as booking_count,
    sum(case when b.booking_status = 'CONFIRMED' then 1 else 0 end) as confirmed_booking_count,
    sum(case when b.booking_status = 'CONFIRMED' then b.ticket_price else 0 end) as confirmed_revenue,
    sum(case when b.is_refunded then b.ticket_price else 0 end) as refund_amount
from {{ ref('fact_booking') }} b
group by 1, 2, 3
