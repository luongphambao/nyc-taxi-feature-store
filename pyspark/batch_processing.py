import argparse
import os
import dotenv
from pydeequ.profiles import ColumnProfilerRunner
from pyspark.sql import SparkSession
dotenv.load_dotenv(".env")


def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "jars/postgresql-42.4.3.jar,jars/deequ-2.0.3-spark-3.3.jar,jars/trino-jdbc-434.jar",
        )
        .appName("Python Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )
    print("Spark session created")
    # Read from PostgreSQL database via jdbc connection
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    df = (
        spark.read.format("jdbc")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("url", f"jdbc:trino://localhost:{os.getenv('TRINO_PORT')}/")
        .option(
            "dbtable", os.getenv("TRINO_DBTABLE", "datalake.taxi_time_series.nyc_taxi")
        )
        .option("user", os.getenv("TRINO_USER", "trino"))
        .option("password", os.getenv("TRINO_PASSWORD", ""))
        .load()
    )
    columns = df.columns
    print(columns)
    # Distribution of payment_type
    print(df.groupBy("payment_type").count().show())
    # Distribution of passenger_count
    print(df.groupBy("passenger_count").count().show())
    # check missing values


if __name__ == "__main__":
    main()
