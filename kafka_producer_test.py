
import json
from datetime import datetime, timezone

from kafka import KafkaProducer

# Connect Python to our Kafka broker
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

# Sample normalized log event
log_event = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "level": "ERROR",
    "service": "payment_api",
    "message": "Payment processing failed",
    "attributes": {
        "error_code": "PAYMENT_TIMEOUT"
    },
    "metadata": {
        "source": "python_test",
        "format": "json"
    }
}

try:
    # Send the event to our Kafka topic
    future = producer.send("normalized-logs", value=log_event)

    # Wait for Kafka to acknowledge the message
    record_metadata = future.get(timeout=10)

    print("Log event sent successfully!")
    print("Topic:", record_metadata.topic)
    print("Partition:", record_metadata.partition)
    print("Offset:", record_metadata.offset)

except Exception as error:
    print("Failed to send log event:", error)

finally:
    producer.flush()
    producer.close()
