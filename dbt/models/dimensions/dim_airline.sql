select distinct
    airline,
    airline as airline_code
from {{ ref('stg_flights') }}
where airline is not null
