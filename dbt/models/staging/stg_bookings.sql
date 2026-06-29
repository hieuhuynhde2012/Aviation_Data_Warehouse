with source as (
    select
        trim(upper(booking_id)) as booking_id,
        nullif(trim(upper(customer_id)), '') as customer_id,
        trim(upper(flight_id)) as flight_id,
        flight_date,
        trim(upper(airline)) as airline,
        regexp_replace(trim(upper(origin_airport)), '-T[0-9]+$', '') as origin_airport,
        regexp_replace(trim(upper(destination_airport)), '-T[0-9]+$', '') as destination_airport,
        booking_time,
        trim(upper(replace(booking_channel, ' ', '_'))) as booking_channel_raw,
        trim(upper(replace(customer_segment, ' ', '_'))) as customer_segment_raw,
        abs(ticket_price) as ticket_price,
        trim(upper(currency)) as currency,
        trim(upper(replace(booking_status, ' ', '_'))) as booking_status_raw,
        trim(upper(replace(payment_status, ' ', '_'))) as payment_status_raw,
        case
            when lower(trim(is_refunded::text)) in ('true', 't', '1', 'yes', 'y') then true
            else false
        end as is_refunded,
        created_at,
        greatest(updated_at, created_at) as updated_at,
        loaded_at
    from {{ source('raw', 'raw_bookings') }}
    where nullif(trim(booking_id), '') is not null
),
standardized as (
    select
        booking_id,
        coalesce(customer_id, 'UNKNOWN_CUSTOMER') as customer_id,
        flight_id,
        flight_date,
        airline,
        origin_airport,
        destination_airport,
        concat(origin_airport, '-', destination_airport) as route,
        booking_time,
        case
            when booking_channel_raw in ('APP', 'MOBILE', 'MOBILE_APP') then 'MOBILE_APP'
            when booking_channel_raw in ('CALLCENTER', 'CALL_CENTER', 'PHONE') then 'CALL_CENTER'
            when booking_channel_raw in ('AGENT', 'AGENCY') then 'AGENCY'
            else 'WEB'
        end as booking_channel,
        case
            when customer_segment_raw in ('BUSINESS', 'CORPORATE') then 'BUSINESS'
            when customer_segment_raw in ('LOYAL', 'LOYALTY') then 'LOYALTY'
            when customer_segment_raw in ('RETURNING', 'REPEAT') then 'RETURNING'
            else 'NEW'
        end as customer_segment,
        ticket_price,
        currency,
        case
            when booking_status_raw in ('CONFIRMED', 'CONF', 'BOOKED', 'COMPLETE') then 'CONFIRMED'
            when booking_status_raw in ('CANCELLED', 'CANCELED', 'VOID', 'CNCL') then 'CANCELLED'
            when booking_status_raw in ('PENDING', 'PEND', 'IN_PROGRESS', 'AWAITING_PAYMENT') then 'PENDING'
            else 'PENDING'
        end as booking_status,
        case
            when payment_status_raw in ('PAID', 'SUCCESS', 'SETTLED') then 'PAID'
            when payment_status_raw in ('FAILED', 'DECLINED', 'ERROR', 'FAIL') then 'FAILED'
            when payment_status_raw in ('REFUNDED', 'REFUND', 'CHARGEBACK') then 'REFUNDED'
            else 'FAILED'
        end as payment_status,
        is_refunded,
        created_at,
        updated_at,
        loaded_at,
        row_number() over (
            partition by booking_id
            order by updated_at desc, loaded_at desc
        ) as row_num
    from source
)
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
    updated_at,
    loaded_at
from standardized
where row_num = 1
