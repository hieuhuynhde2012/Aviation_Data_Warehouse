select
    r.route,
    r.origin_airport,
    r.destination_airport,
    sum(coalesce(b.booking_count, 0)) as booking_count,
    sum(coalesce(b.confirmed_revenue, 0)) as revenue,
    avg(r.avg_delay_minutes) as avg_delay_minutes,
    sum(r.delayed_flight_count)::numeric / nullif(sum(r.flight_count), 0) as delayed_rate,
    sum(r.cancelled_flight_count)::numeric / nullif(sum(r.flight_count), 0) as cancellation_rate
from {{ ref('int_route_daily') }} r
left join {{ ref('int_flight_booking_bridge') }} b
    on r.route = b.route and r.flight_date = b.flight_date
group by 1, 2, 3
