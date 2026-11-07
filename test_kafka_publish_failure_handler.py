
import json
import tempfile
from pathlib import Path

import log_collector


class FailingPublisher:
    """Simulate a Kafka publishing failure."""

    def publish(self, event):
        raise RuntimeError("Simulated Kafka publishing failure")


def test_publish_failure_handler():
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        log_dir = root / "logs"
        checkpoint_dir = root / "checkpoints"

        log_dir.mkdir()
        checkpoint_dir.mkdir()

        # Keep the test isolated from real project files.
        log_collector.LOG_DIR = log_dir
        log_collector.CHECKPOINT_DIR = checkpoint_dir

        test_log = log_dir / "payment_api.log"
        event = {
            "timestamp": "2026-10-25T12:45:00",
            "level": "ERROR",
            "service": "payment_api",
            "message": "SIMULATED_PUBLISH_FAILURE_TEST",
        }

        test_log.write_bytes(
            (json.dumps(event) + "\n").encode("utf-8")
        )

        state = log_collector.load_checkpoint(test_log)
        original_offset = state["offset"]

        count = log_collector.collect_new_records(
            test_log,
            state,
            FailingPublisher(),
        )

        assert count == 0, f"Expected 0, got {count}"
        assert state["offset"] == original_offset, (
            "Checkpoint advanced after publishing failed"
        )

        normalized_file = log_dir / "normalized_logs.jsonl"

        if normalized_file.exists():
            saved_text = normalized_file.read_text(encoding="utf-8")
            assert "SIMULATED_PUBLISH_FAILURE_TEST" not in saved_text

        print("\nPASS: Publishing exception was caught.")
        print("PASS: The checkpoint did not advance.")
        print("PASS: The record was not saved as normalized.")
        print("PASS: Failure-handler test completed.")


if __name__ == "__main__":
    test_publish_failure_handler()
