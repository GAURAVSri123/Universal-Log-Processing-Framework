import json
import sqlite3
from datetime import datetime, timezone

from confluent_kafka import Consumer

DB_PATH = "processed_logs.db"

config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "idempotency-replay-demo-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

# Create a local database and a table with a unique record key.
db = sqlite3.connect(DB_PATH)

db.execute("""
    CREATE TABLE IF NOT EXISTS processed_logs (
        event_key TEXT PRIMARY KEY,
        topic TEXT NOT NULL,
        partition_id INTEGER NOT NULL,
        offset_id INTEGER NOT NULL,
        log_data TEXT NOT NULL,
        processed_at TEXT NOT NULL
    )
""")
db.commit()

consumer = Consumer(config)
consumer.subscribe(["normalized-logs"])

print("Idempotent consumer started.")
print("Reading normalized-logs...")
print("Press Ctrl+C to stop.")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print("Kafka error:", message.error())
            continue

        try:
            raw_value = message.value().decode("utf-8")
            event = json.loads(raw_value)

            # A stable key for this specific Kafka record.
            event_key = (
                f"{message.topic()}:"
                f"{message.partition()}:"
                f"{message.offset()}"
            )

            cursor = db.execute(
                """
                INSERT OR IGNORE INTO processed_logs
                (event_key, topic, partition_id, offset_id,
                 log_data, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    json.dumps(event),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

            # Persist the database result before committing Kafka.
            db.commit()

            if cursor.rowcount == 1:
                print("SAVED:", event_key)
            else:
                print("DUPLICATE SKIPPED:", event_key)

            # Commit only after the database write succeeds.
            consumer.commit(
                message=message,
                asynchronous=False,
            )

        except Exception as error:
            print("Processing failed:", error)
            print("Stopping without committing this record.")
            break

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    db.close()
    print("Consumer and database closed.")
