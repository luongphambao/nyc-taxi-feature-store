from pyspark.sql import SparkSession
import argparse
import os
import dotenv
from pydeequ.profiles import ColumnProfilerRunner

print(os.listdir())
dotenv.load_dotenv(".env")
def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "jars/postgresql-42.6.0.jar,jars/deequ-2.0.3-spark-3.3.jar,jars/trino-jdbc-434.jar",
        )
        .appName("Python Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )
    # Read from PostgreSQL database via jdbc connection
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    print(os.getenv("TRINO_DBTABLE"))
    print(os.getenv("TRINO_USER"))
    print(os.getenv("TRINO_PASSWORD"))
    #select * from datalake.taxi_time_series.nyc_taxi 

    df = (
        spark.read.format("jdbc")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("url", f"jdbc:trino://localhost:{os.getenv('TRINO_PORT')}/")
        .option("dbtable", os.getenv("TRINO_DBTABLE"))
        .option("user", os.getenv("TRINO_USER"))
        .option("password", os.getenv("TRINO_PASSWORD",""))
        .load()
    )
    print(df.show(4))
    columns = df.columns
    print(columns)
    #Distribution of payment_type
    print(df.groupBy("payment_type").count().show())
    #Distribution of passenger_count
    print(df.groupBy("passenger_count").count().show())
    #check missing values


if __name__ == "__main__":
    main()
