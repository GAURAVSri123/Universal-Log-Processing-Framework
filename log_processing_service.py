import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

from log_parser import validate_record_detailed
from kafka_publisher import LogKafkaPublisher


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

INPUT_TOPIC = "normalized-logs"
OUTPUT_TOPIC = "processed-logs"
DLQ_TOPIC = "dead-letter-logs"
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "log-processing-service",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}


# ---------------------------------------------------------
# Log classification
# ---------------------------------------------------------

CLASSIFICATION_RULES = {
    "CRITICAL": {
        "classification": "SYSTEM_FAILURE",
        "priority": "P1",
    },
    "ERROR": {
        "classification": "APPLICATION_ERROR",
        "priority": "P2",
    },
    "WARNING": {
        "classification": "POTENTIAL_ISSUE",
        "priority": "P3",
    },
    "INFO": {
        "classification": "NORMAL_OPERATION",
        "priority": "P4",
    },
    "DEBUG": {
        "classification": "DIAGNOSTIC_INFORMATION",
        "priority": "P5",
    },
}


# ---------------------------------------------------------
# Processing function
# ---------------------------------------------------------

def process_log_event(log_event):

    if not isinstance(log_event, dict):
        raise ValueError("Log event must be a JSON object")

    # Reuse the existing parser's validation rules.
    normalized, error = validate_record_detailed(log_event)

    if error is not None:
        raise ValueError(f"Invalid log event: {error}")

    # Preserve all original fields, including raw and metadata.
    processed_event = dict(log_event)

    # Use the validated severity.
    level = normalized["level"]

    classification = CLASSIFICATION_RULES[level]

    # Add processing information.
    processed_event["level"] = level
    processed_event["classification"] = (
        classification["classification"]
    )
    processed_event["priority"] = classification["priority"]

    processed_event["processing"] = {
        "status": "PROCESSED",
        "processed_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "processor": "log_processing_service",
    }

    return processed_event


# ---------------------------------------------------------
# Main service
# ---------------------------------------------------------


def main():

    consumer = Consumer(consumer_config)

    publisher = None
    dlq_publisher = None

    try:
        publisher = LogKafkaPublisher(topic=OUTPUT_TOPIC)
        dlq_publisher = LogKafkaPublisher(topic=DLQ_TOPIC)

        consumer.subscribe([INPUT_TOPIC])

        print("=" * 55)
        print("LOG PROCESSING SERVICE STARTED")
        print("Input topic :", INPUT_TOPIC)
        print("Output topic:", OUTPUT_TOPIC)
        print("DLQ topic   :", DLQ_TOPIC)
        print("=" * 55)

        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            try:
                # Step 1: Decode and validate the incoming event.
                try:
                    raw_value = message.value().decode("utf-8")
                    log_event = json.loads(raw_value)

                    processed_event = process_log_event(log_event)

                except (ValueError, UnicodeDecodeError) as error:

                    # Step 2: Build a Dead Letter Queue record.
                    dlq_event = {
                        "original_value": (
                            message.value().decode(
                                "utf-8",
                                errors="replace"
                            )
                        ),
                        "failure": {
                            "error": str(error),
                            "failed_at": datetime.now(
                                timezone.utc
                            ).isoformat(),
                            "source_topic": message.topic(),
                            "source_partition": message.partition(),
                            "source_offset": message.offset(),
                        },
                    }

                    # Step 3: Publish the failed event.
                    result = dlq_publisher.publish(dlq_event)

                    print("\n[DLQ]")
                    print("Reason:", str(error))
                    print(
                        "Published to:",
                        result["topic"],
                        "Partition:",
                        result["partition"],
                        "Offset:",
                        result["offset"],
                    )

                    # Step 4: Commit only after DLQ publication.
                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )

                    print("[COMMITTED] Invalid event safely moved to DLQ.")

                    continue

                # Step 5: Publish valid processed events.
                result = publisher.publish(processed_event)

                print("\n[PROCESSED]")
                print("Service:", processed_event["service"])
                print("Level:", processed_event["level"])
                print(
                    "Classification:",
                    processed_event["classification"],
                )
                print("Priority:", processed_event["priority"])

                print(
                    "Published to:",
                    result["topic"],
                    "Partition:",
                    result["partition"],
                    "Offset:",
                    result["offset"],
                )

                # Step 6: Commit after successful publication.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                print("[COMMITTED] Input offset committed.")

            except Exception as error:

                print("\n[PROCESSING FAILED]")
                print(error)
                print("Input offset was not committed.")
                print("Stopping service for safe recovery.")
                break

    except KeyboardInterrupt:
        print("\nStopping processing service...")

    finally:

        consumer.close()

        if publisher is not None:
            publisher.close()

        if dlq_publisher is not None:
            dlq_publisher.close()

        print("Processing service stopped.")


if __name__ == "__main__":
    main()
