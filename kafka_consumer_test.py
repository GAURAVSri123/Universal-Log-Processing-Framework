
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "normalized-logs",
    bootstrap_servers=["localhost:9092"],
    group_id="log-framework-test-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("Waiting for log events... Press Ctrl+C to stop.")

try:
    for message in consumer:
        print("\nReceived log event:")
        print(json.dumps(message.value, indent=2))
        print("Partition:", message.partition)
        print("Offset:", message.offset)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
