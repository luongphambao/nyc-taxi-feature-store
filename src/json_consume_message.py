from confluent_kafka import Consumer, KafkaException
import json

def main():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest' # try latest to get the recent value
    })

    consumer.subscribe(['nyc_taxi_0'])

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
                value=json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {value}")
    except KeyboardInterrupt:
        print('Aborted by user!\n')

    finally:
        # Close consumer
        consumer.close()

if __name__ == '__main__':
    main()