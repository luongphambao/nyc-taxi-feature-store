# How-to Guide



### 2. Push data to MinIO
```shell
python src/export_data_to_datalake.py
```

**Note:** Don't forget to install dependencies from `requirements.txt` first.

## Create data schema
After putting your files to `MinIO``, please execute `trino` container by the following command:
```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an DOUBLEeractive mode

After that, run the following command to register a new schema for our data:

```sql
CREATE SCHEMA IF NOT EXISTS datalake.taxi_time_series
WITH (location = 's3://taxi-time-series/');

CREATE TABLE IF NOT EXISTS datalake.taxi_time_series.nyc_taxi (
  VendorID INT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count DOUBLE,
  trip_distance DOUBLE,
  RatecodeID DOUBLE, 
  store_and_fwd_flag VARCHAR(5), 
  PULocationID INT,
  DOLocationID INT, 
  payment_type INT, 
  fare_amount DOUBLE, 
  extra DOUBLE, 
  mta_tax DOUBLE, 
  tip_amount DOUBLE, 
  tolls_amount DOUBLE, 
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  congestion_surcharge DOUBLE, 
  Airport_fee DOUBLE
) WITH (
  external_location = 's3://taxi-time-series/nyc_taxi',
  format = 'PARQUET'
);
```

## Query with DBeaver
1. Install `DBeaver` as in the following [guide](https://dbeaver.io/download/)
2. Connect to our database (type `trino`) using the following information (empty `password`):
  ![DBeaver Trino](./imgs/trino.png)