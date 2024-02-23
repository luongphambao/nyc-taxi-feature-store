import os
import dotenv

from pyspark.sql import SparkSession

from pydeequ.checks import *
from pydeequ.verification import *

from pydeequ.suggestions import *

from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    Size,
    Completeness,
)

dotenv.load_dotenv(".env")


def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "./jars/postgresql-42.6.0.jar,./jars/deequ-2.0.3-spark-3.3.jar",
        )
        .appName("Python Spark SQL basic example")
        .getOrCreate()
    )

    # Read from PostgreSQL database via jdbc connection
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql:{os.getenv('POSTGRES_DB')}")
        .option("dbtable", "public.devices")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD"))
        .load()
    )

    # Profile data with Deequ
    # https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/
    # https://pydeequ.readthedocs.io/en/latest/README.html
    profile_result = ColumnProfilerRunner(spark).onData(df).run()

    # Give information on completeness, min, max, mean, and sum, .etc.
    for col, profile in profile_result.profiles.items():
        if col == "index":
            continue

        print("*" * 30)
        print(f"Column: {col}")
        print(profile)

    # Calculate some other statistics (we call them metrics)
    analysis_result = (
        AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("velocity"))
        .run()
    )

    analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(
        spark, analysis_result
    )
    analysisResult_df.show()

    # Ok, after analyzing the data, we want to verify the properties 
    # we have collected also applied to a new dataset
    check = Check(spark, CheckLevel.Error, "Review Check") # Another level is Error

    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.isComplete("velocity")  \
            .isNonNegative("velocity")) \
        .run()

    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    checkResult_df.show()

if __name__ == "__main__":
    main()
