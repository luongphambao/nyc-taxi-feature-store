import os

from pyflink.table import *
from pyflink.table.expressions import col

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"

# environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode()
)
t_env.get_config().set(
    "pipeline.jars",
    f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-table-api-java-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-confluent-registry-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-1.17.1.jar;"
    + f"file://{JARS_PATH}/avro-1.11.1.jar;"
    + f"file://{JARS_PATH}/jackson-databind-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-core-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-annotations-2.14.2.jar;"
    + f"file://{JARS_PATH}/kafka-schema-registry-client-5.3.0.jar;"
    + f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
)

# register Orders table and Result table sink in table environment
source_ddl = f"""
    CREATE TABLE nyctaxi (
        nyc_taxi_id INT,
        created STRING,
        vendorid INT NOT NULL,
        tpep_pickup_datetime TIMESTAMP NOT NULL,
        tpep_dropoff_datetime TIMESTAMP NOT NULL,
        passenger_count FLOAT,
        trip_distance FLOAT,
        ratecodeid FLOAT,
        store_and_fwd_flag CHAR(1),
        pulocationid INT,
        dolocationID INT,
        payment_type INT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'nyc_taxi_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json'
    )
    """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    CREATE TABLE sink_nyctaxi (
        nyc_taxi_id INT,
        created STRING,
        passenger_count FLOAT,
        trip_distance FLOAT,    
        payment_type INT,
        total_amount FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sink_nyctaxi_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
nyctaxi = t_env.from_path("nyctaxi")
nyctaxi.select(
    col("nyc_taxi_id"),
    col("created"),
    col("passenger_count"),
    col("trip_distance"),
    col("payment_type"),
    col("total_amount"),
).execute_insert("sink_nyctaxi").wait()

# devices.select(col("device_id"), col("created"), col("feature_3")).execute_insert(
#     "sink_device"
# ).wait()
