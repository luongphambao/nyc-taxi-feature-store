WITH yellow_taxi AS (
    SELECT DISTINCT
        store_and_fwd_flag
    FROM {{ ref('yellow_taxi') }}
)

SELECT
    store_and_fwd_flag,
    {{ decode_memory('store_and_fwd_flag') }} as memory_type
FROM
    yellow_taxi
WHERE
    store_and_fwd_flag IS NOT NULL
ORDER BY
    store_and_fwd_flag ASC