select *
from {{ ref('fact_booking') }}
where ticket_price < 0
