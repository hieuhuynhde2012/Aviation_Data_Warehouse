{{ config(
    materialized='incremental',
    unique_key='booking_id',
    pre_hook="{{ delete_fact_booking_partitions() }}"
) }}

select
    booking_id,
    customer_id,
    flight_id,
    flight_date,
    airline,
    origin_airport,
    destination_airport,
    route,
    booking_time,
    booking_channel,
    customer_segment,
    ticket_price,
    currency,
    booking_status,
    payment_status,
    is_refunded,
    created_at,
    updated_at
from {{ ref('stg_bookings') }}
