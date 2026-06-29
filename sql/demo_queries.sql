-- Demo queries for the Aviation Data Warehouse portfolio project.
-- Run from the host:
-- Get-Content sql/demo_queries.sql | docker compose exec -T postgres-warehouse psql -U aviation -d aviation_dw
--
-- Or paste individual queries into psql / DBeaver / Superset SQL Lab.

-- 1. Latest source-to-raw reconciliation status.
with latest_run as (
    select run_id
    from metadata.reconciliation_summary
    order by created_at desc
    limit 1
)
select
    source_name,
    source_file_rows,
    quarantined_rows,
    expected_raw_rows,
    actual_raw_rows,
    difference,
    status
from metadata.reconciliation_summary
where run_id = (select run_id from latest_run)
order by source_name;

-- 2. Data quality errors by severity and rule.
select
    severity,
    rule_name,
    count(*) as record_count
from metadata.dq_record_errors
group by severity, rule_name
order by record_count desc;

-- 3. Recent quarantined records, including the raw JSON payload.
select
    created_at,
    run_id,
    source_name,
    record_key,
    reason,
    raw_record
from quarantine.invalid_records
order by created_at desc
limit 10;

-- 4. Idempotent load evidence: reruns with the same checksum should skip inserts.
select
    source_name,
    target_table,
    checksum_md5,
    source_rows,
    valid_rows,
    quarantined_rows,
    inserted_rows,
    skipped_rows,
    load_status,
    completed_at
from metadata.raw_load_audit
order by completed_at desc, source_name
limit 20;

-- 5. Fact table health checks.
select 'fact_booking' as table_name, count(*) as row_count from mart.fact_booking
union all
select 'fact_payment', count(*) from mart.fact_payment
union all
select 'fact_flight_status', count(*) from mart.fact_flight_status;

-- 6. Revenue and booking performance by route.
select
    route,
    booking_count,
    revenue,
    avg_delay_minutes,
    delayed_rate,
    cancellation_rate
from mart.mart_route_performance
order by revenue desc nulls last
limit 20;

-- 7. Daily sales trend.
select
    flight_date,
    sum(booking_count) as bookings,
    sum(revenue) as revenue,
    sum(refund_amount) as refund_amount,
    sum(failed_payment_count) as failed_payment_count
from mart.mart_sales_performance
group by flight_date
order by flight_date
limit 60;

-- 8. Booking channel trend.
select
    booking_date,
    booking_channel,
    sum(booking_count) as bookings,
    sum(confirmed_revenue) as confirmed_revenue,
    sum(cancelled_count) as cancelled_count
from mart.mart_booking_trend_daily
group by booking_date, booking_channel
order by booking_date, booking_channel
limit 80;

-- 9. Customer lifecycle segments.
select
    lifecycle_segment,
    customer_count,
    booking_count,
    total_spend,
    avg_customer_spend
from mart.mart_customer_segment
order by total_spend desc nulls last;

-- 10. Airport operational performance.
select
    airport_code,
    airport_name,
    iso_region,
    origin_volume,
    destination_volume,
    delay_count,
    cancelled_count
from mart.mart_airport_performance
order by delay_count desc nulls last
limit 25;

-- 11. Flight delay drivers by airline and route.
select
    flight_date,
    airline,
    route,
    flight_count,
    avg_delay_minutes,
    delayed_rate,
    carrier_delay_minutes,
    weather_delay_minutes,
    nas_delay_minutes,
    late_aircraft_delay_minutes
from mart.mart_flight_delay_analysis
order by avg_delay_minutes desc nulls last
limit 25;

-- 12. Orphan-check after cross-source quarantine and dbt relationship tests.
select count(*) as orphan_payment_count
from mart.fact_payment p
left join mart.fact_booking b
    on p.booking_id = b.booking_id
where b.booking_id is null;
