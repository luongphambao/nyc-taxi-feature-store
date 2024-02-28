{{ config(materialized='table') }}

{% set table_columns = ["vendorid", "passenger_count","trip_distance","total_amount","payment_type","ratecodeid","store_and_fwd_flag"] %}

with source_data as (
    select vendorid,passenger_count,trip_distance,total_amount,payment_type,ratecodeid,store_and_fwd_flag
    from nyc_taxi_warehouse
)

select *
from source_data