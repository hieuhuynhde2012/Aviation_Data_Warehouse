select
    flight_date,
    airline,
    route,
    count(*) as booking_count,
    sum(case when booking_status = 'CONFIRMED' then ticket_price else 0 end) as revenue,
    avg(ticket_price) as average_ticket_value,
    sum(case when is_refunded then ticket_price else 0 end) as refund_amount,
    sum(case when payment_status = 'FAILED' then 1 else 0 end) as failed_payment_count
from {{ ref('fact_booking') }}
group by 1, 2, 3
