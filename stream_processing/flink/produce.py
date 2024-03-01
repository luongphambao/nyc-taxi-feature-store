import argparse
import io
import json
from datetime import datetime
from time import sleep

import numpy as np
import pandas as pd
from bson import json_util
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-c",
    "--schemas_path",
    default="./avro_schemas",
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

# Define some constants
NUM_nyc_taxiS = 1


def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def create_streams(servers, schemas_path):
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass
    df = pd.read_parquet("streamming_data.parquet")
    # print(df.head())
    while True:
        record = {}
        # Make event one more year recent to simulate fresher data
        record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record["nyc_taxi_id"] = np.random.randint(low=0, high=NUM_nyc_taxiS)

        # Read columns from schema
        schema_path = f"{schemas_path}/schema_{record['nyc_taxi_id']}.avsc"
        with open(schema_path, "r") as f:
            parsed_schema = json.loads(f.read())
        value_sample = df.sample(1).values[0]
        # print(value_sample)
        index_feature = 0
        for field in parsed_schema["fields"]:

            # select value from random row
            # value=df[field["name"]].sample(1).values[0]

            if field["name"] not in ["created", "nyc_taxi_id"]:
                record[field["name"]] = value_sample[index_feature]
                index_feature += 1
        # print(record)
        # Get topic name for this nyc_taxi
        topic_name = f'nyc_taxi_{record["nyc_taxi_id"]}'

        # Create a new topic for this nyc_taxi id if not exists
        create_topic(admin, topic_name=topic_name)

        # Send messages to this topic
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        print(record)
        sleep(2)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for nyc_taxi_id in range(NUM_nyc_taxiS):
        try:
            teardown_stream(f"nyc_taxi_{nyc_taxi_id}", [servers])
        except Exception as e:
            print(f"Topic nyc_taxi_{nyc_taxi_id} does not exist. Skipping...!")

    if mode == "setup":
        schemas_path = parsed_args["schemas_path"]
        create_streams([servers], schemas_path)
