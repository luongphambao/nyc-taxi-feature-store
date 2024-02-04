
-- Use the `ref` function to select from other models

select *
from {{ ref('yellow_taxi') }}
where trip_distance< 1.5