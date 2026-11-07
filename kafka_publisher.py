
import json
import time

from kafka import KafkaProducer
from kafka.serializer import Serializer


class JSONValueSerializer(Serializer):
    """Serialize Python objects into UTF-8 JSON bytes."""

    def serialize(self, topic, headers, data):

        if data is None:
            return None

        return json.dumps(
            data,
            ensure_ascii=False
        ).encode("utf-8")


class LogKafkaPublisher:
    """Publishes log records and diagnosis events to Kafka."""

    def __init__(
        self,
        bootstrap_servers="localhost:9092",
        topic="normalized-logs",
    ):

        self.topic = topic
        self.producer = None
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):

            try:
                print(
                    f"[KAFKA] Connecting to Kafka "
                    f"(attempt {attempt}/{max_attempts})..."
                )

                self.producer = KafkaProducer(
                    bootstrap_servers=[bootstrap_servers],

                    value_serializer=JSONValueSerializer(),

                    acks="all",
                    retries=5,
                    enable_idempotence=True,
                )

                print("[KAFKA] Connected successfully.")
                break

            except Exception as error:

                print(
                    f"[KAFKA] Connection attempt {attempt} failed: "
                    f"{error}"
                )

                if attempt == max_attempts:
                    raise RuntimeError(
                        "Could not connect to Kafka after "
                        f"{max_attempts} attempts."
                    ) from error

                time.sleep(2)

    def publish(self, event, key=None):
        """Publish an event and wait for broker acknowledgement."""

        message_key = (
            key.encode("utf-8")
            if key is not None
            else None
        )

        future = self.producer.send(
            self.topic,
            value=event,
            key=message_key
        )

        metadata = future.get(timeout=10)

        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

    def close(self):
        """Flush pending messages and close the producer."""

        if self.producer is not None:
            self.producer.flush()
            self.producer.close()
            self.producer = None
