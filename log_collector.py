from pathlib import Path
import json
import time
from log_parser import parse_by_source,mask_failed_record
from kafka_publisher import LogKafkaPublisher

def get_file_identity(log_path):
    """Return metadata that helps identify a physical file."""
    file_stat = log_path.stat()

    return {
        "device": file_stat.st_dev,
        "inode": file_stat.st_ino,
    }

# Project directories
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"



def discover_log_files():
    """Discover all top-level .log files in the logs directory."""
    if not LOG_DIR.exists():
        return []

    return sorted(
        (
            path
            for path in LOG_DIR.iterdir()
            if path.is_file()
            and (
                path.name.endswith(".log")
                or ".log." in path.name
            )
        ),
        key=lambda path: path.name,
    )



LOG_FILES = discover_log_files()

print("Discovered log files:", [path.name for path in LOG_FILES])


POLL_INTERVAL = 1  # Check for new records every second


def find_checkpoint_by_identity(log_path, current_identity):
    """Find a saved checkpoint belonging to the same physical file."""
    if current_identity is None:
        return None

    for checkpoint_path in CHECKPOINT_DIR.glob("*.json"):
        try:
            with checkpoint_path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            # A checkpoint must be a JSON object (dictionary).
            if not isinstance(data, dict):
                continue

            offset = data.get("offset")

            if (
                isinstance(data, dict)
                and data.get("identity") == current_identity
                and type(offset) is int
                and offset >= 0
            ):
                print(
                    f"[CHECKPOINT MIGRATION] {checkpoint_path.name} "
                    f"matches {log_path.name}. "
                    f"Recovered offset {offset}."
                )

                # Create a checkpoint under the new filename.
                if save_checkpoint(log_path, offset, current_identity):
                    return {
                        "offset": offset,
                        "identity": current_identity,
                    }

        except (OSError, json.JSONDecodeError):
            continue

    return None



def load_checkpoint(log_path):
    """Load offset and file identity from the checkpoint."""
    checkpoint_path = CHECKPOINT_DIR / f"{log_path.name}.json"

    current_identity = (
        get_file_identity(log_path) if log_path.exists() else None
    )

    
    if not checkpoint_path.exists():
        recovered = find_checkpoint_by_identity(
            log_path, current_identity
        )

        if recovered is not None:
            return recovered

        return {
            "offset": 0,
            "identity": current_identity,
        }


    try:
        with checkpoint_path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if not isinstance(data,dict):
            raise ValueError("Checkpoint must contain a json object")

        offset = data.get("offset")
        saved_identity = data.get("identity")

        if (
            data.get("filename") != log_path.name
            or type(offset) is not int
            or offset < 0
        ):
            raise ValueError("Invalid checkpoint data")

        # Old checkpoints don't contain identity information.
        if not isinstance(saved_identity, dict):
            print(
                f"[WARNING] {log_path.name}: checkpoint has no "
                "file identity. Starting from byte 0."
            )
            return {
                "offset": 0,
                "identity": current_identity,
            }

        # The filename is the same, but the physical file may differ.
        if (
            current_identity is not None
            and saved_identity != current_identity
        ):
            print(
                f"[ROTATION] {log_path.name} has been replaced. "
                "Starting from byte 0."
            )
            return {
                "offset": 0,
                "identity": current_identity,
            }

        print(
            f"[CHECKPOINT] Resuming {log_path.name} "
            f"from byte {offset}"
        )
        return {
            "offset": offset,
            "identity": saved_identity,
        }

    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(
            f"[WARNING] Cannot load checkpoint for "
            f"{log_path.name}: {error}"
        )
        return {
            "offset": 0,
            "identity": current_identity,
        }

def save_checkpoint(log_path, offset, identity):
    """Save offset and file identity safely."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    checkpoint_path = CHECKPOINT_DIR / f"{log_path.name}.json"
    temp_path = CHECKPOINT_DIR / f"{log_path.name}.tmp"

    data = {
        "filename": log_path.name,
        "offset": offset,
        "identity": identity,
    }

    try:
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
            file.flush()

        temp_path.replace(checkpoint_path)
        return True

    except OSError as error:
        print(
            f"\n[WARNING] Could not save checkpoint for "
            f"{log_path.name}: {error}"
        )
        return False



def save_failed_log(source_file, raw_record, error):
    """Persist a log record that could not be parsed."""
    failed_logs_path = LOG_DIR / "failed_logs.jsonl"

    failed_entry = {
        "source_file": source_file,
        "raw": mask_failed_record(raw_record),
        "error": error,
    }

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        with failed_logs_path.open("a", encoding="utf-8") as file:
            file.write(
                json.dumps(failed_entry, ensure_ascii=False) + "\n"
            )
            file.flush()

        return True

    except OSError as error:
        print(f"\n[WARNING] Could not save failed log: {error}")
        return False


def save_normalized_log(source_file, record):
    """Persist a successfully normalized log record."""
    normalized_logs_path = LOG_DIR / "normalized_logs.jsonl"

    # Keep the original filename with the normalized fields.
    normalized_entry = {
        "source_file": source_file,
        **record,
    }

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        with normalized_logs_path.open("a", encoding="utf-8") as file:
            file.write(
                json.dumps(normalized_entry, ensure_ascii=False) + "\n"
            )
            file.flush()

        return True

    except OSError as write_error:
        print(
            f"\n[WARNING] Could not save normalized log: {write_error}"
        )
        return False



def collect_new_records(log_path, state,publisher):
    """Read complete new lines and checkpoint their byte position."""
    try:
        if not log_path.exists():
            return 0

        # Detect whether the current path refers to a different file.
        current_identity = get_file_identity(log_path)

        if state["identity"] != current_identity:
            print(
                f"\n[ROTATION] {log_path.name} identity changed. "
                "Restarting from byte 0."
            )

            state["offset"] = 0
            state["identity"] = current_identity

            if not save_checkpoint(log_path, 0, current_identity):
                print("[WARNING] Checkpoint reset was not saved.")

        file_size = log_path.stat().st_size

        # Handle truncation of the same file.
        if file_size < state["offset"]:
            print(
                f"\n[RESET] {log_path.name} was truncated. "
                "Restarting from byte 0."
            )

            state["offset"] = 0

            if not save_checkpoint(log_path, 0, state["identity"]):
                print("[WARNING] Checkpoint reset was not saved.")

        start_offset = state["offset"]

        # Read new bytes from the saved position.
        with log_path.open("rb") as file:
            file.seek(start_offset)
            new_data = file.read()

        if not new_data:
            return 0

        # Process only complete lines ending in a newline.
        last_newline = new_data.rfind(b"\n")

        if last_newline == -1:
            return 0

        complete_data = new_data[:last_newline + 1]
        raw_lines = complete_data.split(b"\n")[:-1]

                # Process complete log lines one by one.
        processed_offset = start_offset
        processed_count = 0

        # demo_app.log and demo_app.log.1 map to "demo_app".
        source_name = log_path.name.split(".log", 1)[0]

        for raw_line in raw_lines:
            # Calculate the byte position, including the newline.
            processed_offset += len(raw_line) + 1

            # Remove Windows carriage return before decoding.
            raw_record = raw_line.removesuffix(b"\r").decode(
                "utf-8",
                errors="replace",
            )

            result = parse_by_source(raw_record, source_name)

            
            if result["status"] == "SUCCESS":
                record = result["record"]

                # First, publish the normalized record to Kafka.
                try:
                    kafka_result = publisher.publish(record)
                    print(
                        f"\n[KAFKA] Published {log_path.name}: "
                        f"topic={kafka_result['topic']}, "
                        f"partition={kafka_result['partition']}, "
                        f"offset={kafka_result['offset']}"
                    )
                except Exception as error:
                    print(
                        f"\n[WARNING] Kafka publishing failed for "
                        f"{log_path.name}: {error}"
                    )
                    print(
                        "[WARNING] Keeping the previous checkpoint "
                        "so this record can be retried."
                    )
                    return processed_count

                # Preserve the existing local JSONL output.
                if not save_normalized_log(log_path.name, record):
                    print(
                        "[WARNING] Normalized log was not saved. "
                        "Keeping the previous checkpoint."
                    )
                    return processed_count

                print(
                    f"\n[NORMALIZED] {log_path.name}: "
                    f"{json.dumps(record, ensure_ascii=False)}"
                )


            else:
                # Preserve invalid or unrecognized records.
                print(
                    f"\n[PARSE ERROR] {log_path.name}: "
                    f"{result['error']}"
                )
                print(f"[RAW LOG] {raw_record}")

                if not save_failed_log(
                    log_path.name,
                    raw_record,
                    result["error"],
                ):
                    print(
                        "[WARNING] Failed log was not saved. "
                        "Keeping the previous checkpoint."
                    )
                    return processed_count

            # A record is persisted. Now save progress through this line.
            if save_checkpoint(
                log_path,
                processed_offset,
                state["identity"],
            ):
                state["offset"] = processed_offset
            else:
                print(
                    f"[WARNING] Checkpoint for {log_path.name} "
                    "was not saved; this record may be processed again."
                )
                return processed_count + 1

            processed_count += 1

        return processed_count

    except (PermissionError, OSError) as error:
        print(f"\n[WARNING] {log_path.name}: {error}")
        return 0




def main():
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    publisher = None
    total_records = 0

    try:
        publisher = LogKafkaPublisher()

        # Load checkpoints for files present at startup.
        states = {
            path: load_checkpoint(path)
            for path in LOG_FILES
        }

        print("=" * 60)
        print("UNIVERSAL LOG PROCESSING FRAMEWORK")
        print("Continuous Collector - Persistent Checkpointing")
        print("=" * 60)
        print("\nMonitoring log files. Press Ctrl+C to stop.")

        while True:
            # Discover files again on every polling cycle.
            discovered_files = discover_log_files()

            # First: initialize every newly discovered file.
            for path in discovered_files:
                if path not in states:
                    print(f"\n[DISCOVERY] New log file: {path.name}")
                    states[path] = load_checkpoint(path)

            # Second: collect records only after initialization.
            for path in discovered_files:
                count = collect_new_records(
                    path,
                    states[path],
                    publisher,
                )
                total_records += count

            # Report files that disappeared, but retain checkpoints.
            for path in list(states):
                if path not in discovered_files and not path.exists():
                    print(
                        f"\n[REMOVED] Log file is no longer present: "
                        f"{path.name}"
                    )
                    del states[path]

            print(
                f"\rRecords collected this run: {total_records}",
                end="",
                flush=True,
            )

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nCollector stopped by user.")
        print(f"Records collected this run: {total_records}")

    except Exception as error:
        print(f"\n[ERROR] Collector stopped: {error}")

    
    finally:
        if publisher is not None:
            try:
                publisher.close()
                print("Kafka publisher closed.")
            except Exception as error:
                print(f"[WARNING] Error closing Kafka publisher: {error}")



if __name__ == "__main__":
    main()
