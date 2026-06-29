select
    trim(upper(coalesce(nullif(iata_code, ''), gps_code, ident))) as airport_code,
    trim(upper(ident)) as ident,
    trim(lower(type)) as airport_type,
    trim(name) as airport_name,
    latitude_deg,
    longitude_deg,
    elevation_ft,
    trim(upper(iso_country)) as iso_country,
    trim(upper(iso_region)) as iso_region,
    nullif(trim(municipality), '') as municipality,
    trim(lower(scheduled_service)) as scheduled_service,
    loaded_at
from {{ source('raw', 'raw_airports') }}
where coalesce(nullif(iata_code, ''), gps_code, ident) is not null
