import argparse
import os
from glob import glob
import dotenv
import pandas as pd
dotenv.load_dotenv(".env")
import time
from pyspark.sql import Row, SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "jars/postgresql-42.4.3.jar",
    )
    # .config("spark.some.config.option", "some-value")
    # config large file batchsize 1000000 rows reWriteBatchedInserts=true
    # .config("spark.sql.execution.arrow.maxRecordsPerBatch", "200000")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .appName("Python Spark read parquet example")
    .getOrCreate()
)
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
print(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB)
start_time = time.time()
for file in os.listdir("data/"):
    if file.endswith(".parquet"):
        parquet_path = "data/" + file
        print("Reading parquet file: ", parquet_path)
    else:
        continue
    df = spark.read.parquet(parquet_path)
    print(df.show(4))
    # print len of df
    print("Number of rows: ", df.count())

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
