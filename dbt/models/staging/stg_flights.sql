select
    md5(concat_ws('|', reporting_airline, flight_number_reporting_airline, flight_date, origin, dest)) as flight_id,
    flight_date,
    reporting_airline as airline,
    tail_number,
    flight_number_reporting_airline as flight_number,
    origin as origin_airport,
    dest as destination_airport,
    concat(origin, '-', dest) as route,
    crs_dep_time,
    dep_time,
    coalesce(dep_delay_minutes, 0) as dep_delay_minutes,
    crs_arr_time,
    arr_time,
    coalesce(arr_delay_minutes, 0) as arr_delay_minutes,
    coalesce(cancelled, 0)::integer as cancelled,
    cancellation_code,
    coalesce(diverted, 0)::integer as diverted,
    coalesce(distance, 0) as distance_miles,
    coalesce(carrier_delay, 0) as carrier_delay,
    coalesce(weather_delay, 0) as weather_delay,
    coalesce(nas_delay, 0) as nas_delay,
    coalesce(security_delay, 0) as security_delay,
    coalesce(late_aircraft_delay, 0) as late_aircraft_delay,
    loaded_at
from {{ source('raw', 'raw_bts_flights') }}
where flight_date is not null
  and origin is not null
  and dest is not null
