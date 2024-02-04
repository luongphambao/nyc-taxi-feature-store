
-- Use the `ref` function to select from other models

select *
from {{ ref('yellow_taxi') }}
where total_amount<20