import argparse
import json

from confluent_kafka import Consumer, KafkaException


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topic", type=str, default="nyc_taxi.public.nyc_taxi", help="topic to consume"
    )
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    topic = args.topic
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "mygroup",
            "auto.offset.reset": "earliest",  # try latest to get the recent value
        }
    )

    consumer.subscribe([topic])

    # Read messages from Kafka
    try:
        while True:
            # Wait for up to 1 second for new messages to arrive
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Parse data from our message
                print(f"Received message: {msg.value().decode('utf-8')}")
                try:
                    value = json.loads(msg.value().decode("utf-8"))["payload"]["after"]
                except:
                    value = msg.value().decode("utf-8")
                print(f"Received message: {value}")
    except KeyboardInterrupt:
        print("Aborted by user!\n")

    finally:
        # Close consumer
        consumer.close()


if __name__ == "__main__":
    main()
