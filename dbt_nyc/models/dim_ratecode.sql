WITH ratecode_stg AS(
    SELECT DISTINCT 
        ratecodeid
    FROM {{ ref('yellow_taxi') }}
)

SELECT 
    ratecodeid,
    {{ decode_ratecode('ratecodeid') }} as rate_name
FROM 
    ratecode_stg
WHERE
    ratecodeid IS NOT NULL 
    AND
    cast(ratecodeid as integer) < 7
ORDER BY
    ratecodeid ASC