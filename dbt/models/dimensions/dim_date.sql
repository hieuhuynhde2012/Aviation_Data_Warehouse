with dates as (
    select distinct flight_date as date_day from {{ ref('stg_flights') }}
    union
    select distinct flight_date as date_day from {{ ref('stg_bookings') }}
)
select
    date_day,
    extract(year from date_day)::integer as year,
    extract(month from date_day)::integer as month,
    extract(day from date_day)::integer as day,
    extract(dow from date_day)::integer as day_of_week,
    extract(quarter from date_day)::integer as quarter
from dates
where date_day is not null
