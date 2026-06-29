with source as (
    select
        trim(upper(payment_id)) as payment_id,
        trim(upper(booking_id)) as booking_id,
        payment_time,
        trim(upper(replace(payment_method, ' ', '_'))) as payment_method_raw,
        trim(upper(replace(payment_status, ' ', '_'))) as payment_status_raw,
        greatest(amount, 0) as amount,
        trim(upper(currency)) as currency,
        greatest(refund_amount, 0) as refund_amount,
        created_at,
        greatest(updated_at, created_at) as updated_at,
        loaded_at
    from {{ source('raw', 'raw_payments') }}
    where nullif(trim(payment_id), '') is not null
),
standardized as (
    select
        payment_id,
        booking_id,
        payment_time,
        case
            when payment_method_raw in ('CREDIT_CARD', 'CC', 'VISA', 'MASTERCARD') then 'CREDIT_CARD'
            when payment_method_raw in ('DEBIT_CARD', 'DC') then 'DEBIT_CARD'
            when payment_method_raw in ('PAYPAL', 'PAY_PAL') then 'PAYPAL'
            when payment_method_raw in ('BANK_TRANSFER', 'TRANSFER') then 'BANK_TRANSFER'
            else 'UNKNOWN'
        end as payment_method,
        case
            when payment_status_raw in ('PAID', 'SUCCESS', 'SETTLED') then 'PAID'
            when payment_status_raw in ('FAILED', 'DECLINED', 'ERROR', 'FAIL') then 'FAILED'
            when payment_status_raw in ('REFUNDED', 'REFUND', 'CHARGEBACK') then 'REFUNDED'
            else 'FAILED'
        end as payment_status,
        amount,
        currency,
        refund_amount,
        created_at,
        updated_at,
        loaded_at,
        row_number() over (
            partition by payment_id
            order by updated_at desc, loaded_at desc
        ) as row_num
    from source
)
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
    updated_at,
    loaded_at
from standardized
where row_num = 1
