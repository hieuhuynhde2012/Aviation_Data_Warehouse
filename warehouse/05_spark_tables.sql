create table if not exists mart.spark_route_delay_features (
    route text not null,
    airline text not null,
    flight_count bigint,
    cancelled_count bigint,
    diverted_count bigint,
    avg_dep_delay_minutes numeric,
    avg_arr_delay_minutes numeric,
    p95_arr_delay_minutes numeric,
    avg_distance_miles numeric,
    carrier_delay_minutes numeric,
    weather_delay_minutes numeric,
    nas_delay_minutes numeric,
    security_delay_minutes numeric,
    late_aircraft_delay_minutes numeric,
    spark_processed_at timestamp,
    primary key (route, airline)
);

create table if not exists mart.spark_route_booking_features (
    route text primary key,
    booking_event_count bigint,
    confirmed_booking_events bigint,
    cancelled_booking_events bigint,
    pending_booking_events bigint,
    distinct_customers bigint,
    total_ticket_value numeric,
    avg_ticket_price numeric,
    web_booking_events bigint,
    mobile_booking_events bigint,
    agency_booking_events bigint,
    call_center_booking_events bigint,
    spark_processed_at timestamp
);

create table if not exists metadata.spark_job_audit (
    job_run_id text primary key,
    job_name text not null,
    source_flight_rows bigint,
    source_booking_rows bigint,
    output_route_delay_rows bigint,
    output_route_booking_rows bigint,
    status text not null,
    started_at timestamp,
    completed_at timestamp,
    error_message text
);
