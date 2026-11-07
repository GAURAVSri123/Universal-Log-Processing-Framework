
import json
from confluent_kafka import Consumer, KafkaException

# 1. Configure the consumer
config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "log-processing-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# 2. Create the consumer
consumer = Consumer(config)

# 3. Subscribe to our topic
consumer.subscribe(["normalized-logs"])

print("Kafka consumer started...")
print("Waiting for log events. Press Ctrl+C to stop.")

try:
    while True:
        # 4. Wait for a message
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            raise KafkaException(message.error())

        # 5. Decode the Kafka message
        raw_value = message.value().decode("utf-8")

        # 6. Convert JSON text into a Python dictionary
        log_event = json.loads(raw_value)

        # 7. Process the log event
        print("\nLog received:")
        print(json.dumps(log_event, indent=4))

        print("Topic:", message.topic())
        print("Partition:", message.partition())
        print("Offset:", message.offset())

        # 8. Commit only after successful processing
        consumer.commit(
            message=message,
            asynchronous=False
        )

        print("Offset committed successfully.")

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")

finally:
    consumer.close()
    print("Consumer closed.")
