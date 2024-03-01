import json
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

JARS_PATH = f"{os.getcwd()}/jars/"
print(JARS_PATH)


def merge_features(record):
    """
    Merged feature columns into one single data column
    and keep other columns unchanged.
    """
    # Convert Row to dict
    record = json.loads(record)

    # Create a dictionary of all features
    # and create a data column for this
    data = {}
    for key in record:
        if key != "created" and key != "content":
            data[key] = record[key]

    # Convert the data column to string
    # and add other features back to record
    return json.dumps(
        {
            "created": record["created"],
            "data": data,
        }
    )


def print_features(record):
    """
    Merged feature columns into one single data column
    and keep other columns unchanged.
    """
    # Convert Row to dict
    record = json.loads(record)
    data = record["payload"]["after"]
    # print(data)
    return json.dumps(data)


def check_record_keys(record):
    """
    Check messages have "payload" key or not
    """
    # Convert Row to dict
    record = json.loads(record)
    keys = list(record.keys())
    if "payload" in keys:
        return True

    print("record: ", list(record.keys()))
    return True


def filter_features(record):
    """
    Remove unnecessary columns
    """
    record = json.loads(record)
    data = {}
    for key in record:
        if key != "created" and key != "content":
            data[key] = record[key]

    return json.dumps({"created": record["created"], "data": data})


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )

    # Avro will need it for validation from the schema registry
    # schema_path = "./data_ingestion/kafka_producer/avro_schemas/schema_0.avsc"
    # with open(schema_path) as f:
    #     schema = f.read()

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("nyc_taxi.public.nyc_taxi")
        .set_group_id("nyc_taxi-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("nyc_taxi.sink.datastream")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # No sink, just print out to the terminal
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
    #     filter_small_features
    # ).map(merge_features).print()

    # Add a sink to be more industrial, remember to cast to STRING in map
    # it will not work if you don't do it
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
        check_record_keys
    ).map(print_features, output_type=Types.STRING()).map(
        filter_features, output_type=Types.STRING()
    ).sink_to(
        sink=sink
    )
    env.execute("flink_datastream_demo1")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
