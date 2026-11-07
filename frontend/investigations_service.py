"""Read-only access to recent AI diagnosis events from Kafka."""

import json
from datetime import datetime, timezone
from confluent_kafka import (
    Consumer,
    KafkaException,
    TopicPartition,
)


class InvestigationsService:
    """Fetch recent diagnosis events without committing production offsets."""

    def __init__(self, bootstrap_servers="localhost:9092", topic="ai-diagnosis"):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def fetch_investigations(
        self,
        limit=500,
        idle_timeout=1.0,
        max_read_seconds=8,
    ):
        """Read a bounded window of recent messages from every partition."""

        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": "ai-investigations-dashboard-readonly",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "latest",
            "allow.auto.create.topics": False,
        })

        records = []
        errors = []

        try:
            metadata = consumer.list_topics(self.topic, timeout=5)
            topic_metadata = metadata.topics.get(self.topic)

            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(
                    f"Kafka topic '{self.topic}' was not found. "
                    "Start Kafka and the AI diagnosis pipeline first."
                )

            partitions = sorted(topic_metadata.partitions.keys())

            if not partitions:
                return [], []

            # Divide the requested history across all topic partitions.
            per_partition_limit = max(1, (limit + len(partitions) - 1) // len(partitions))
            assignments = []

            for partition in partitions:
                partition_ref = TopicPartition(self.topic, partition)

                low, high = consumer.get_watermark_offsets(
                    partition_ref,
                    timeout=5,
                    cached=False,
                )

                # Start near the latest retained messages instead of offset 0.
                start_offset = max(low, high - per_partition_limit)

                if start_offset < high:
                    assignments.append(
                        TopicPartition(self.topic, partition, start_offset)
                    )

            if not assignments:
                return [], []

            consumer.assign(assignments)

            started_at = datetime.now(timezone.utc).timestamp()
            last_message_at = started_at

            while len(records) < limit:
                now = datetime.now(timezone.utc).timestamp()

                if now - started_at >= max_read_seconds:
                    break

                message = consumer.poll(0.25)

                if message is None:
                    if (
                        datetime.now(timezone.utc).timestamp()
                        - last_message_at
                        >= idle_timeout
                    ):
                        break
                    continue

                if message.error():
                    errors.append(str(message.error()))
                    continue

                last_message_at = datetime.now(timezone.utc).timestamp()

                try:
                    event = json.loads(message.value().decode("utf-8"))

                    if isinstance(event, dict):
                        event.setdefault("_kafka", {})
                        event["_kafka"].update({
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                        })
                        records.append(event)

                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    errors.append(
                        f"Could not parse message at partition "
                        f"{message.partition()}, offset {message.offset()}: {exc}"
                    )

        except (KafkaException, RuntimeError, Exception) as exc:
            errors.append(str(exc))

        finally:
            consumer.close()

        records.sort(
            key=lambda item: (
                item.get("analyzed_at")
                or (item.get("log") or {}).get("timestamp", "")
            ),
            reverse=True,
        )

        return records, errors