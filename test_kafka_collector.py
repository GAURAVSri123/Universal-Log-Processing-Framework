
import json
import tempfile
from pathlib import Path

import log_collector
from kafka_publisher import LogKafkaPublisher


def test_kafka_collector():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create isolated folders for this test.
        log_dir = temp_path / "logs"
        checkpoint_dir = temp_path / "checkpoints"
        log_dir.mkdir()
        checkpoint_dir.mkdir()

        # Redirect the collector to temporary folders.
        log_collector.LOG_DIR = log_dir
        log_collector.CHECKPOINT_DIR = checkpoint_dir

        # Create one uniquely identifiable test log.
        test_log = log_dir / "payment_api.log"
        event = {
            "timestamp": "2026-10-25T12:00:00",
            "level": "ERROR",
            "service": "payment_api",
            "message": "KAFKA_COLLECTOR_INTEGRATION_TEST_251025",
            "transaction_id": "test-001",
        }

        test_log.write_bytes(
            (json.dumps(event) + "\n").encode("utf-8")
        )

        publisher = LogKafkaPublisher()

        try:
            state = log_collector.load_checkpoint(test_log)

            count = log_collector.collect_new_records(
                test_log,
                state,
                publisher,
            )

            normalized_file = log_dir / "normalized_logs.jsonl"
            checkpoint_file = checkpoint_dir / (
                test_log.name + ".json"
            )

            assert count == 1, f"Expected 1 record, got {count}"
            assert normalized_file.exists(), (
                "Normalized output file was not created"
            )
            assert checkpoint_file.exists(), (
                "Checkpoint file was not created"
            )

            saved = [
                json.loads(line)
                for line in normalized_file.read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.strip()
            ]

            assert len(saved) == 1, (
                f"Expected 1 normalized record, got {len(saved)}"
            )

            print("\nPASS: Collector processed the test record.")
            print("PASS: Normalized record was saved locally.")
            print("PASS: Checkpoint was saved.")
            print("Check the Kafka topic for the unique test message.")

        finally:
            publisher.close()


if __name__ == "__main__":
    test_kafka_collector()
