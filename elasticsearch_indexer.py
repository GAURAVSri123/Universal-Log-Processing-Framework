
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaException


# ---------------- CONFIGURATION ----------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "processed-logs"
KAFKA_GROUP_ID = "elasticsearch-indexer"

ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "processed-logs"


# ---------------- ELASTICSEARCH SETUP ----------------

def create_elasticsearch_index(es):
    """Create the log index if it does not already exist."""

    if not es.indices.exists(index=ELASTICSEARCH_INDEX):
        es.indices.create(
            index=ELASTICSEARCH_INDEX,
            mappings={
                "properties": {
                    "timestamp": {"type": "date"},
                    "processed_at": {"type": "date"},
                    "level": {"type": "keyword"},
                    "service": {"type": "keyword"},
                    "message": {"type": "text"},
                    "classification": {"type": "keyword"},
                    "priority": {"type": "keyword"},
                    "attributes": {"type": "object", "dynamic": True}
                }
            }
        )
        print(f"[ELASTICSEARCH] Created index: {ELASTICSEARCH_INDEX}")
    else:
        print(f"[ELASTICSEARCH] Index already exists: {ELASTICSEARCH_INDEX}")


# ---------------- KAFKA CONSUMER ----------------

def main():
    es = Elasticsearch(ELASTICSEARCH_URL)

    if not es.ping():
        raise RuntimeError("Cannot connect to Elasticsearch.")

    print("[ELASTICSEARCH] Connected successfully.")
    create_elasticsearch_index(es)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([KAFKA_TOPIC])
    print(f"[KAFKA] Listening to topic: {KAFKA_TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"[KAFKA ERROR] {message.error()}")
                continue

            try:
                log_event = json.loads(message.value().decode("utf-8"))
                # Normalize timestamps before indexing into Elasticsearch.
                timestamp_value = log_event.get("timestamp")

                if not isinstance(timestamp_value, str) or not timestamp_value.strip():
                    raise ValueError("Missing or invalid timestamp.")

                try:
                    # Handle ISO 8601 timestamps.
                    parsed_timestamp = datetime.fromisoformat(
                        timestamp_value.replace("Z", "+00:00")
                    )

                except ValueError:
                    try:
                        # Handle older Python logging timestamps.
                        parsed_timestamp = datetime.strptime(
                            timestamp_value,
                            "%Y-%m-%d %H:%M:%S,%f"
                        )

                    except ValueError:
                        raise ValueError(
                            f"Unsupported timestamp format: {timestamp_value}"
                        )

                log_event["timestamp"] = parsed_timestamp.isoformat()

                # Stable ID prevents duplicate documents when
                # the same Kafka record is processed again.
                document_id = (
                    f"{message.topic()}-"
                    f"{message.partition()}-"
                    f"{message.offset()}"
                )

                es.index(
                    index=ELASTICSEARCH_INDEX,
                    id=document_id,
                    document=log_event
                )

                # Commit only after Elasticsearch confirms indexing.
                consumer.commit(message=message, asynchronous=False)

                print(
                    f"[INDEXED] {document_id} | "
                    f"{log_event.get('level', 'UNKNOWN')} | "
                    f"{log_event.get('service', 'UNKNOWN')}"
                )

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                print(f"[INVALID MESSAGE] {error}")
                print("[STOPPING] Offset not committed; inspect this message.")
                break

            except Exception as error:
                print(f"[PROCESSING ERROR] {error}")
                print("[STOPPING] Offset not committed for safe recovery.")
                break

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping Elasticsearch indexer...")

    finally:
        consumer.close()
        es.close()
        print("[SHUTDOWN] Indexer stopped.")


if __name__ == "__main__":
    main()
