import os
import pandas as pd

data_sample = "data/2018/yellow_tripdata_2018-01.parquet"
df = pd.read_parquet(data_sample)
columns = df.columns
print(df["congestion_surcharge"].dtype)
print(df["passenger_count"].dtype)
print(df["passenger_count"].unique())
scripts = "CREATE TABLE IF NOT EXISTS datalake.iot_time_series.nyc-taxi("
for col in columns:
    if col == "pickup_datetime" or col == "dropoff_datetime":
        scripts += col + " TIMESTAMP,"
    elif df[col].dtype == "int64":
        scripts += col + " INT,"
    elif df[col].dtype == "float64":
        scripts += col + " DOUBLE,"
    else:
        scripts += col + " VARCHAR(5),"
scripts = scripts[:-1] + ")"
print(scripts)


# create scripts for create schema and table
# schema
# ```sql
# CREATE SCHEMA IF NOT EXISTS datalake.iot_time_series
# WITH (location = 's3://iot-time-series/');

# CREATE TABLE IF NOT EXISTS datalake.iot_time_series.nyc-taxi(
#     vendor_id INT,
#     pickup_datetime TIMESTAMP,
#     dropoff_datetime TIMESTAMP,
#     passenger_count INT,
#     trip_distance FLOAT,
#     rate_code_id INT,
#     store_and_fwd_flag VARCHAR(5),
#     payment_type INT,
#     fare_amount FLOAT,
#     extra FLOAT,
#     mta_tax FLOAT,
#     tip_amount FLOAT,
#     tolls_amount FLOAT,
#     improvement_surcharge FLOAT,
#     total_amount FLOAT
# )
