select
    lifecycle_segment,
    count(*) as customer_count,
    sum(booking_count) as booking_count,
    sum(total_spend) as total_spend,
    avg(total_spend) as avg_customer_spend
from {{ ref('int_customer_booking') }}
group by 1
