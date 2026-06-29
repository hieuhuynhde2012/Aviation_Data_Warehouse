-- depends_on: {{ ref('stg_bookings') }}

{{ config(
    materialized='incremental',
    unique_key=['booking_date', 'booking_hour', 'booking_channel'],
    pre_hook="{{ delete_booking_trend_partitions() }}"
) }}

select
    booking_time::date as booking_date,
    extract(hour from booking_time)::integer as booking_hour,
    booking_channel,
    count(*) as booking_count,
    sum(case when booking_status = 'CONFIRMED' then ticket_price else 0 end) as confirmed_revenue,
    sum(case when booking_status = 'CANCELLED' then 1 else 0 end) as cancelled_count
from {{ ref('fact_booking') }}
group by 1, 2, 3
