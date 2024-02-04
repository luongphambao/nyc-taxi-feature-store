WITH payment_stg AS(
    SELECT DISTINCT 
        payment_type
    FROM {{ ref('yellow_taxi') }}
)

SELECT 
    payment_type,
    {{ decode_payment('payment_type') }} as method_payment
FROM 
    payment_stg
WHERE
    payment_type IS NOT NULL
ORDER BY
    payment_type ASC