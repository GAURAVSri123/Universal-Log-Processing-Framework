
from confluent_kafka import Consumer

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "log-processing-demo-group",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

consumer = Consumer(config)
consumer.subscribe(["normalized-logs"])

print("Consumer group demo started.")
print("Waiting for partition assignment and new logs...")

previous_assignment = None

try:
    while True:
        message = consumer.poll(1.0)

        # Check which partitions Kafka assigned to this consumer.
        assignment = sorted(
            (p.topic, p.partition)
            for p in consumer.assignment()
        )

        if assignment != previous_assignment and assignment:
            print("\nAssigned partitions:")
            for topic, partition in assignment:
                print(f"  {topic} -> partition {partition}")

            previous_assignment = assignment

        if message is None:
            continue

        if message.error():
            print("Kafka error:", message.error())
            continue

        print(
            f"Received: partition={message.partition()}, "
            f"offset={message.offset()}, "
            f"value={message.value().decode('utf-8')}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    print("Consumer closed.")
