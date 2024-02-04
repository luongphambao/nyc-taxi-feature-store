WITH vendor_stg AS(
    SELECT DISTINCT 
        vendorid
    FROM {{ ref('yellow_taxi') }}
)

SELECT
    vendorid,
    {{ decode_vendor('vendorid') }} as vendor_name
FROM vendor_stg
WHERE vendorid IS NOT NULL AND cast(vendorid as integer) < 3
ORDER BY vendorid ASC