
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException

from kafka_publisher import LogKafkaPublisher
from rag_pipeline import RAGPipeline


# Kafka configuration
INPUT_TOPIC = "processed-logs"
OUTPUT_TOPIC = "ai-diagnosis"
DLQ_TOPIC = "ai-diagnosis-dlq"

CONSUMER_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ai-diagnosis-service",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 900000,
}

# Only these logs will be sent for AI diagnosis.
DIAGNOSIS_LEVELS = {"ERROR", "CRITICAL"}

MAX_AI_ATTEMPTS = 3


def build_source_id(message):
    """
    Generate a stable identity for the source Kafka message.
    """

    return (
        f"{message.topic()}:"
        f"{message.partition()}:"
        f"{message.offset()}"
    )


def publish_to_dlq(publisher, message, error):
    """
    Publish a failed message to the AI dead-letter topic.
    """

    original_value = message.value().decode(
        "utf-8",
        errors="replace"
    )

    dlq_event = {
        "original_value": original_value,
        "failure": {
            "error": str(error),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
            "processor": "ai_diagnosis_service"
        }
    }

    publisher.publish(
        dlq_event,
        key=build_source_id(message)
    )


def main():

    consumer = Consumer(CONSUMER_CONFIG)

    diagnosis_publisher = None
    dlq_publisher = None
    pipeline = None

    try:

        print("[AI SERVICE] Initializing RAG pipeline...")

        # Load the embedding model only once.
        pipeline = RAGPipeline(top_k=3)

        # Ensure the existing vector index is available.
        pipeline.retriever.vector_store.create_index()

        diagnosis_publisher = LogKafkaPublisher(
            topic=OUTPUT_TOPIC
        )

        dlq_publisher = LogKafkaPublisher(
            topic=DLQ_TOPIC
        )

        consumer.subscribe([INPUT_TOPIC])

        print("\n[AI SERVICE] Started successfully.")
        print(f"[KAFKA] Listening to: {INPUT_TOPIC}")

        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            source_id = build_source_id(message)

            try:

                # Step 1: Decode the Kafka message.
                raw_value = message.value().decode("utf-8")
                log_event = json.loads(raw_value)

                if not isinstance(log_event, dict):
                    raise ValueError(
                        "Kafka message must contain a JSON object."
                    )
                
                # Normalize the timestamp before AI analysis and indexing.
                timestamp_value = log_event.get("timestamp")

                if not isinstance(timestamp_value, str) or not timestamp_value.strip():
                    raise ValueError("Missing or invalid timestamp.")

                try:
                    # First, try ISO 8601 for mat.
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

                # Step 2: Check whether AI analysis is required.
                level = str(
                    log_event.get("level", "")
                ).upper()

                if level not in DIAGNOSIS_LEVELS:

                    print(
                        f"[SKIPPED] {source_id} | "
                        f"Level: {level or 'UNKNOWN'}"
                    )

                    consumer.commit(
                        message=message,
                        asynchronous=False
                    )

                    continue

                print(
                    f"\n[AI SERVICE] Analyzing {source_id}"
                )

                # Step 3: Execute the existing RAG pipeline.
                analysis_result = None
                last_error = None

                for attempt in range(1, MAX_AI_ATTEMPTS + 1):

                    try:

                        print(
                            f"[AI SERVICE] Analysis attempt "
                            f"{attempt}/{MAX_AI_ATTEMPTS}"
                        )

                        analysis_result = pipeline.analyze_log(
                            log_event
                        )

                        diagnosis = analysis_result.get(
                            "diagnosis"
                        )

                        if not isinstance(diagnosis, dict):
                            raise ValueError(
                                "Invalid diagnosis response."
                            )

                        if diagnosis.get("parse_error"):
                            raise ValueError(
                                "LLM returned invalid JSON: "
                                + diagnosis["parse_error"]
                            )

                        last_error = None
                        break
                        

                    except Exception as error:

                        last_error = error

                        print(
                            f"[AI SERVICE] Attempt {attempt} failed: "
                            f"{error}"
                        )

                        if attempt < MAX_AI_ATTEMPTS:
                            time.sleep(2 * attempt)

                # Step 4: Send unsuccessful analysis to the DLQ.
                if last_error is not None:

                    publish_to_dlq(
                        dlq_publisher,
                        message,
                        last_error
                    )

                    consumer.commit(
                        message=message,
                        asynchronous=False
                    )

                    print(
                        f"[AI SERVICE] Sent to DLQ: {source_id}"
                    )

                    continue

                diagnosis = analysis_result["diagnosis"]

                # Step 5: Build the structured diagnosis event.
                # Do not include raw log attributes.
                diagnosis_event = {
                    "event_id": source_id,
                    "source": {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset()
                    },
                    "analyzed_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "log": {
                        "service": log_event.get("service"),
                        "level": level,
                        "classification": log_event.get(
                            "classification"
                        ),
                        "priority": log_event.get("priority"),
                        "message": log_event.get("message"),
                        "timestamp": log_event.get("timestamp")
                    },
                    "diagnosis": diagnosis,
                    "diagnosis_status": "COMPLETED"
                }

                # Step 6: Publish the diagnosis.
                diagnosis_publisher.publish(
                    diagnosis_event,
                    key=source_id
                )

                print(
                    f"[AI SERVICE] Diagnosis published: {source_id}"
                )

                # Step 7: Add the current error to the knowledge base.
                # This happens AFTER retrieval to prevent self-matching.
                embedding_result = (
                    pipeline.retriever.embedding_generator
                    .generate_log_embedding(log_event)
                )

                pipeline.retriever.vector_store.index_log(
                    log=log_event,
                    embedding_result=embedding_result,
                    document_id=f"processed-logs:{source_id}",
                    source_index=INPUT_TOPIC,
                    source_id=source_id
                )

                # Step 8: Commit only after successful publication
                # and successful vector indexing.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                print(
                    f"[AI SERVICE] Completed: {source_id}"
                )
                

            except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:

                # Invalid input goes to the AI dead-letter topic.
                publish_to_dlq(
                    dlq_publisher,
                    message,
                    error
                )

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                print(
                    f"[AI SERVICE] Invalid message sent to DLQ: "
                    f"{source_id}"
                )

            except Exception as error:

                # Do not commit if publication or indexing fails.
                print(
                    f"[AI SERVICE ERROR] {error}"
                )

                print(
                    "[AI SERVICE] Offset not committed. "
                    "Stopping for safe recovery."
                )

                break

    except KeyboardInterrupt:

        print("\n[AI SERVICE] Shutdown requested.")

    finally:

        consumer.close()

        if diagnosis_publisher is not None:
            diagnosis_publisher.close()

        if dlq_publisher is not None:
            dlq_publisher.close()

        if pipeline is not None:
            pipeline.close()

        print("[AI SERVICE] Resources closed.")


if __name__ == "__main__":
    main()
