select
    payment_id,
    booking_id,
    payment_time,
    payment_method,
    payment_status,
    amount,
    currency,
    refund_amount,
    created_at,
    updated_at
from {{ ref('stg_payments') }}
