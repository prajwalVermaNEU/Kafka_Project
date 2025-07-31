from confluent_kafka import Consumer
import sys

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'log-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['log_topic'])

print("Consumer is listening to 'log_topic'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print(f"[{msg.key().decode('utf-8')}] {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("Exiting...")
finally:
    consumer.close()
