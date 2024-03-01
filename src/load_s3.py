import os

# from helpers import load_cfg
from glob import glob

# s3://taxi-time-series/nyc_taxi
from minio import Minio

from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "jars/aws-java-sdk-bundle-1.12.262.jar,jars/hadoop-aws-3.3.4.jar",
    )
    # .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4")
    .appName("Python Spark SQL basic example")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
print("spark session created")


def main():

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    endpoint = "localhost:9000"
    access_key = "minio_access_key"
    secret_key = "minio_secret_key"
    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )
    df = spark.read.parquet(
        "s3a://taxi-time-series/nyc_taxi/yellow_tripdata_2020-01.parquet"
    )
    print(df.show(4))
    print("load data from s3 minio sucess")


if __name__ == "__main__":
    main()
