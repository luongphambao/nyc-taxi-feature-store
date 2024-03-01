import os
import time
import dotenv
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size
from pydeequ.checks import *
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import *
from pydeequ.verification import *
from pyspark.sql import SparkSession
dotenv.load_dotenv(".env")


def main():
    # The entrypoint to access all functions of Spark
    start_time = time.time()
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "jars/postgresql-42.4.3.jar,jars/deequ-2.0.3-spark-3.3.jar,jars/trino-jdbc-434.jar",
        )
        # .config("spark.executor.memory", "70g")
        # .config("spark.driver.memory", "50g")
        # .config("spark./  -6memory.offHeap.enabled",True)
        # .config("spark.memory.offHeap.size","16g")
        .appName("Python Spark SQL basic example")
        .getOrCreate()
    )
    time_taken = time.time() - start_time
    print("Time taken to create spark session: ", time_taken)
    # Read from PostgreSQL database via jdbc connection
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    df = (
        spark.read.format("jdbc")
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("url", f"jdbc:trino://localhost:{os.getenv('TRINO_PORT')}/")
        .option(
            "dbtable", os.getenv("TRINO_DBTABLE", "datalake.taxi_time_series.nyc_taxi")
        )
        .option("user", os.getenv("TRINO_USER"))
        .option("password", os.getenv("TRINO_PASSWORD", ""))
        .load()
    )
    print("Connect to Trino database and read data successfully!")
    print(df.show(4))
    columns = df.columns
    print(columns)
    print("Time taken to read data from Trino: ", time.time() - start_time)
    # Profile data with Deequ
    # https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/
    # https://pydeequ.readthedocs.io/en/latest/README.html
    # df = df.limit(4000000)
    profile_result = ColumnProfilerRunner(spark).onData(df).run()

    # Give information on completeness, min, max, mean, and sum, .etc.
    for col, profile in profile_result.profiles.items():
        if col == "index":
            continue

        print("*" * 30)
        print(f"Column: {col}")
        print(profile)
    print("Time taken to profile data: ", time.time() - start_time)
    # Calculate some other statistics (we call them metrics)
    analysis_result = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("passenger_count"))
        .addAnalyzer(Completeness("trip_distance"))
        .run()
    )

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(
        spark, analysis_result
    )
    analysisResult_df.show()

    # Ok, after analyzing the data, we want to verify the properties
    # we have collected also applied to a new dataset
    check = Check(spark, CheckLevel.Error, "Review Check")  # Another level is Error

    checkResult = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check.isComplete("passenger_count").isNonNegative("passenger_count"))
        .addCheck(check.isComplete("trip_distance").isNonNegative("trip_distance"))
        .run()
    )

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    csv_result = checkResult_df.show()
    print("Time taken to check data: ", time.time() - start_time)
    csv_result = checkResult_df.toPandas().to_csv("checkResult_df.csv")


if __name__ == "__main__":
    main()
