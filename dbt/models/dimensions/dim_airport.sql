with ranked as (
    select
        airport_code,
        ident,
        airport_type,
        airport_name,
        latitude_deg,
        longitude_deg,
        elevation_ft,
        iso_country,
        iso_region,
        municipality,
        scheduled_service,
        row_number() over (
            partition by airport_code
            order by case when scheduled_service = 'yes' then 0 else 1 end, ident
        ) as row_num
    from {{ ref('stg_airports') }}
)
select
    airport_code,
    ident,
    airport_type,
    airport_name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    iso_country,
    iso_region,
    municipality,
    scheduled_service
from ranked
where row_num = 1
