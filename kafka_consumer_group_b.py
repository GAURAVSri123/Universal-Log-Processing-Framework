from confluent_kafka import Consumer

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ai-analysis-group",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
}

consumer = Consumer(config)
consumer.subscribe(["normalized-logs"])

print("AI analysis consumer started.")
print("Waiting for new log events...")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print("Kafka error:", message.error())
            continue

        log_event = message.value().decode("utf-8")

        print(
            f"Received from topic={message.topic()}, "
            f"partition={message.partition()}, "
            f"offset={message.offset()}"
        )
        print("Log event:", log_event)

except KeyboardInterrupt:
    print("\nStopping AI consumer...")

finally:
    consumer.close()
    print("AI consumer closed.")
