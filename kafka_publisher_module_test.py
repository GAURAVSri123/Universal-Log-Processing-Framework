
from datetime import datetime, timezone

from kafka_publisher import LogKafkaPublisher

publisher = LogKafkaPublisher()

event = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "level": "INFO",
    "service": "collector_test",
    "message": "Testing the reusable Kafka publisher",
    "attributes": {},
    "metadata": {
        "source": "publisher_module_test",
        "format": "json",
    },
}

try:
    result = publisher.publish(event)
    print("Published successfully:", result)

except Exception as error:
    print("Publishing failed:", repr(error))

finally:
    publisher.close()
