{% macro delete_fact_booking_partitions() %}
    {% if is_incremental() %}
        delete from {{ this }}
        where flight_date in (select distinct flight_date from {{ ref('stg_bookings') }});
    {% endif %}
{% endmacro %}

{% macro delete_booking_trend_partitions() %}
    {% if is_incremental() %}
        delete from {{ this }}
        where booking_date in (select distinct booking_time::date from {{ ref('stg_bookings') }});
    {% endif %}
{% endmacro %}
