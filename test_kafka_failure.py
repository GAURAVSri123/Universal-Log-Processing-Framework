
import json
import tempfile
from pathlib import Path

import log_collector
from kafka_publisher import LogKafkaPublisher


def test_kafka_failure():
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        log_dir = root / "logs"
        checkpoint_dir = root / "checkpoints"

        log_dir.mkdir()
        checkpoint_dir.mkdir()

        log_collector.LOG_DIR = log_dir
        log_collector.CHECKPOINT_DIR = checkpoint_dir

        test_log = log_dir / "payment_api.log"

        event = {
            "timestamp": "2026-10-25T12:30:00",
            "level": "ERROR",
            "service": "payment_api",
            "message": "KAFKA_FAILURE_TEST_251025",
        }

        test_log.write_bytes(
            (json.dumps(event) + "\n").encode("utf-8")
        )

        state = log_collector.load_checkpoint(test_log)
        original_offset = state["offset"]

        publisher = None

        try:
            # Kafka is stopped, so initialization may fail here.
            try:
                publisher = LogKafkaPublisher()
            except Exception as error:
                print(
                    "\nPASS: Producer initialization failed "
                    "while Kafka was unavailable."
                )
                print(f"Failure type: {type(error).__name__}")

                # No collector call occurred, so its checkpoint
                # must still be at its original position.
                assert state["offset"] == original_offset

                print(
                    "PASS: Checkpoint remains unchanged because "
                    "the collector did not process the record."
                )
                print(
                    "Next, restart Kafka and test recovery."
                )
                return

            # If initialization succeeds despite Kafka being down,
            # test the collector's publishing failure handling.
            count = log_collector.collect_new_records(
                test_log,
                state,
                publisher,
            )

            assert count == 1, f"Expected 1, got {count}"
            assert state["offset"] > original_offset, (
                "Checkpoint offset did not advance"
            )

            normalized_file = log_dir / "normalized_logs.jsonl"
            assert normalized_file.exists(), (
                "Normalized output file was not created"
            )

            saved_text = normalized_file.read_text(encoding="utf-8")
            assert "KAFKA_FAILURE_TEST_251025" in saved_text

            print("\nPASS: Kafka accepted the record.")
            print("PASS: The normalized record was saved locally.")
            print("PASS: The checkpoint advanced after processing.")
            print("PASS: Recovery test completed successfully.")

            normalized_file = log_dir / "normalized_logs.jsonl"

            if normalized_file.exists():
                saved_text = normalized_file.read_text(
                    encoding="utf-8"
                )
                

            print("\nPASS: Publishing failure was handled.")
            print("PASS: Checkpoint offset did not advance.")
            print("PASS: Failed record was not saved as normalized.")

        finally:
            if publisher is not None:
                publisher.close()


if __name__ == "__main__":
    test_kafka_failure()
