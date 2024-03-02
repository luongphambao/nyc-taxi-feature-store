# ref https://github.com/apache/flink/blob/master/flink-python/pyflink/examples/datastream/windowing/session_with_dynamic_gap_window.py
import argparse
import datetime
import json
import os
import sys
from typing import Iterable

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from pyflink.common import Encoder, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import (
    Duration,
    TimestampAssigner,
    WatermarkStrategy,
)
from pyflink.datastream import ProcessWindowFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows


class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp) -> int:
        element = json.loads(element)
        timestamp = int(element["payload"]["after"]["created"])
        return timestamp


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context[TimeWindow],
        elements: Iterable[tuple],
    ) -> Iterable[tuple]:
        data_list = []
        total_amounts = 0
        passenger_counts = 0
        trip_distances = 0
        for e in elements:

            record = json.loads(e)
            data = record["payload"]["after"]
            # print(data)
            total_amount = data["total_amount"]
            total_amounts += total_amount
            passenger_count = data["passenger_count"]
            passenger_counts += passenger_count
            trip_distance = data["trip_distance"]
            trip_distances += trip_distance
        print("final window data", len(elements))

        return [
            json.dumps(
                {
                    "total_amount": total_amounts,
                    "passenger_count": passenger_counts,
                    "trip_distance": trip_distances,
                }
            )
        ]


if __name__ == "__main__":
    JARS_PATH = f"jars"
    servers = "localhost:9092"
    producer = KafkaProducer(bootstrap_servers=servers)
    admin_client = KafkaAdminClient(bootstrap_servers=servers)
    topic_name = "nyc_taxi.sink_window_datastream"
    if topic_name not in admin_client.list_topics():
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics="nyc_taxi.public.nyc_taxi",
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": "localhost:9092", "group.id": "test_group"},
    )
    print("kafka_consumer: ", kafka_consumer)
    watermark_strategy = (
        WatermarkStrategy.for_monotonous_timestamps()
        .with_timestamp_assigner(CustomTimestampAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    stream = env.add_source(kafka_consumer)
    ds = (
        stream.assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(
            lambda x: json.loads(x)["payload"]["after"]["content"],
            key_type=Types.STRING(),
        )
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10000)))
        .process(CountWindowProcessFunction(), output_type=Types.STRING())
        .sink_to(sink=sink)
        .set_parallelism(1)
    )
    env.execute()
