"""Read-only dashboard access to recent dead-letter Kafka messages."""

import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition


DLQ_TOPICS = ("dead-letter-logs", "ai-diagnosis-dlq")

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-dashboard-readonly",
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    "enable.partition.eof": False,
    "allow.auto.create.topics": False,
}


def _decode_value(value):
    if value is None:
        return {}

    try:
        decoded = (
            value.decode("utf-8")
            if isinstance(value, bytes)
            else str(value)
        )
    except Exception:
        return {"raw_value": repr(value)}

    try:
        parsed = json.loads(decoded)
        return parsed if isinstance(parsed, dict) else {"payload": parsed}
    except (json.JSONDecodeError, TypeError):
        return {"raw_value": decoded}


def _decode_original(value):
    """Decode original_value, which may itself contain JSON text."""

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")

    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    return value if value is not None else {}


def _first(data, *keys, default=None):
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return default


def _normalise_record(topic, partition, offset, payload, kafka_timestamp):
    failure = (
        payload.get("failure")
        if isinstance(payload.get("failure"), dict)
        else {}
    )

    source = (
        payload.get("source")
        if isinstance(payload.get("source"), dict)
        else {}
    )

    original = _first(
        payload,
        "original_value",
        "original_message",
        "record",
        "log",
        default={},
    )
    original = _decode_original(original)

    error = _first(
        payload,
        "error",
        "error_message",
        "failure_reason",
        "reason",
        default=_first(
            failure,
            "error",
            "message",
            default="Failure reason not supplied",
        ),
    )

    failed_at = _first(
        payload,
        "failed_at",
        "failedAt",
        "timestamp",
        "created_at",
        default=_first(
            failure,
            "failed_at",
            "timestamp",
            default=None,
        ),
    )

    source_topic = _first(
        payload,
        "source_topic",
        default=_first(
            failure,
            "source_topic",
            default=_first(source, "topic", default=None),
        ),
    )

    source_partition = _first(
        payload,
        "source_partition",
        default=_first(
            failure,
            "source_partition",
            default=_first(source, "partition", default=None),
        ),
    )

    source_offset = _first(
        payload,
        "source_offset",
        default=_first(
            failure,
            "source_offset",
            default=_first(source, "offset", default=None),
        ),
    )

    processor = _first(
        payload,
        "processor",
        "service",
        "component",
        default=_first(failure, "processor", default=None),
    )

    if processor is None:
        processor = {
            "dead-letter-logs": "Log Processing Service (inferred)",
            "ai-diagnosis-dlq": "AI Diagnosis Service (inferred)",
        }.get(topic, topic)

    if failed_at is None and kafka_timestamp and kafka_timestamp[1] is not None:
        failed_at = datetime.fromtimestamp(
            kafka_timestamp[1] / 1000,
            tz=timezone.utc,
        ).isoformat()

    return {
        "dlq_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "failed_at": str(failed_at or "Unknown"),
        "processor": str(processor),
        "error": str(error),
        "source_topic": str(source_topic or "Unknown"),
        "source_partition": str(
            source_partition if source_partition is not None else "Unknown"
        ),
        "source_offset": str(
            source_offset if source_offset is not None else "Unknown"
        ),
        "original": original,
        "payload": payload,
    }


def fetch_dead_letter_records(
    bootstrap_servers="localhost:9092",
    idle_timeout=0.5,
    max_records=500,
):
    """Read recent messages from both DLQ topics without committing offsets."""

    config = dict(CONSUMER_CONFIG)
    config["bootstrap.servers"] = bootstrap_servers

    consumer = Consumer(config)
    records = []

    try:
        metadata = consumer.list_topics(timeout=5)
        assignments = []

        available_partitions = []

        for topic in DLQ_TOPICS:
            topic_meta = metadata.topics.get(topic)

            if topic_meta is None or topic_meta.error is not None:
                continue

            for partition_id in topic_meta.partitions:
                available_partitions.append((topic, partition_id))

        if not available_partitions:
            return records

        per_partition_limit = max(
            1,
            (max_records + len(available_partitions) - 1)
            // len(available_partitions),
        )

        # Read only the most recent retained window from each partition.
        for topic, partition_id in available_partitions:
            partition_ref = TopicPartition(topic, partition_id)

            low, high = consumer.get_watermark_offsets(
                partition_ref,
                timeout=5,
                cached=False,
            )

            start_offset = max(low, high - per_partition_limit)

            if start_offset < high:
                assignments.append(
                    TopicPartition(topic, partition_id, start_offset)
                )

        if not assignments:
            return records

        consumer.assign(assignments)

        idle_rounds = 0

        while len(records) < max_records and idle_rounds < 3:
            message = consumer.poll(0.25)

            if message is None:
                idle_rounds += 1
                continue

            if message.error():
                idle_rounds += 1
                continue

            idle_rounds = 0

            payload = _decode_value(message.value())

            records.append(
                _normalise_record(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    payload,
                    message.timestamp(),
                )
            )

        records.sort(
            key=lambda item: (
                item["failed_at"],
                item["dlq_topic"],
                item["kafka_partition"],
                item["kafka_offset"],
            ),
            reverse=True,
        )

        return records

    except KafkaException:
        raise

    finally:
        consumer.close()