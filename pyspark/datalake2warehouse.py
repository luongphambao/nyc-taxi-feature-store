# Same code as spark_warehouse.py but load data from datalake( using s3)
#you must download aws-java-sdk-bundle-1.12.262.jar before running this code
import argparse
import os
from glob import glob
import dotenv
import pandas as pd
dotenv.load_dotenv(".env")
import time
from pyspark.sql import Row, SparkSession

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
spark = (
    SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "jars/postgresql-42.4.3.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/hadoop-aws-3.3.4.jar",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .appName("Python Spark read parquet example")
    .getOrCreate()
)

print(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB)
start_time = time.time()
list_file_green = [
    "green_tripdata_2021-01.parquet",
    "green_tripdata_2021-02.parquet",
    "green_tripdata_2021-03.parquet",
    "green_tripdata_2021-04.parquet",
    "green_tripdata_2021-05.parquet",
    "green_tripdata_2021-06.parquet",
    "green_tripdata_2021-07.parquet",
    "green_tripdata_2021-08.parquet",
    "green_tripdata_2021-09.parquet",
    "green_tripdata_2021-10.parquet",
    "green_tripdata_2021-11.parquet",
    "green_tripdata_2021-12.parquet",
    "green_tripdata_2022-01.parquet",
    "green_tripdata_2022-02.parquet",
    "green_tripdata_2022-03.parquet",
    "green_tripdata_2022-04.parquet",
    "green_tripdata_2022-05.parquet",
    "green_tripdata_2022-06.parquet",
    "green_tripdata_2022-07.parquet",
    "green_tripdata_2022-08.parquet",
    "green_tripdata_2022-09.parquet",
    "green_tripdata_2022-10.parquet",
    "green_tripdata_2022-11.parquet",
    "green_tripdata_2022-12.parquet",
]
list_file_yellow = [
    "yellow_tripdata_2021-01.parquet",
    "yellow_tripdata_2021-02.parquet",
    "yellow_tripdata_2021-03.parquet",
    "yellow_tripdata_2021-04.parquet",
    "yellow_tripdata_2021-05.parquet",
    "yellow_tripdata_2021-06.parquet",
    "yellow_tripdata_2021-07.parquet",
    "yellow_tripdata_2021-08.parquet",
    "yellow_tripdata_2021-09.parquet",
    "yellow_tripdata_2021-10.parquet",
    "yellow_tripdata_2021-11.parquet",
    "yellow_tripdata_2021-12.parquet",
    "yellow_tripdata_2022-01.parquet",
    "yellow_tripdata_2022-02.parquet",
    "yellow_tripdata_2022-03.parquet",
    "yellow_tripdata_2022-04.parquet",
    "yellow_tripdata_2022-05.parquet",
    "yellow_tripdata_2022-06.parquet",
    "yellow_tripdata_2022-07.parquet",
    "yellow_tripdata_2022-08.parquet",
    "yellow_tripdata_2022-09.parquet",
    "yellow_tripdata_2022-10.parquet",
    "yellow_tripdata_2022-11.parquet",
    "yellow_tripdata_2022-12.parquet",
]
list_file = list_file_green + list_file_yellow
for file in list_file:
    path = "s3a://taxi-time-series/nyc_taxi/" + file
    print("Reading parquet file: ", path)
    df = spark.read.parquet(path)
    DB_TABLE = "nyc_taxi_warehouse"
    df.write.format("jdbc").option("driver", "org.postgresql.Driver").option(
        "url", f"jdbc:postgresql://localhost:5432/{POSTGRES_DB}"
    ).option("dbtable", DB_TABLE).option("user", POSTGRES_USER).option(
        "password", POSTGRES_PASSWORD
    ).option(
        "numPartitions", "10"
    ).option(
        "batchsize", "100000"
    ).mode(
        "append"
    ).save()
    print("Time to save to postgres: ", time.time() - start_time)
