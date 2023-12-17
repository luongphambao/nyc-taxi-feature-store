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
    CREATE TABLE device (
        device_id INT,
        created STRING,
        feature_7 FLOAT,
        feature_4 FLOAT,
        feature_3 FLOAT,
        feature_5 FLOAT,
        feature_8 FLOAT,
        feature_9 FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'device_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json'
    )
    """
t_env.execute_sql(source_ddl)

sink_ddl = f"""
    CREATE TABLE sink_device (
        device_id INT,
        created STRING,
        feature_3 FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sink_device_0',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json'
    )
    """
t_env.execute_sql(sink_ddl)

# specify table program
devices = t_env.from_path("device")

devices.select(col("device_id"), col("created"), col("feature_3")).execute_insert(
    "sink_device"
).wait()
