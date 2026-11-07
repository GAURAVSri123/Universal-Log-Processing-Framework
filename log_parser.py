import re
import json
from pathlib import Path
from datetime import datetime
import socket
from datetime import timezone


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "logs" / "demo_app.log"

VALID_LEVELS = {
    "INFO",
    "WARNING",
    "ERROR",
    "DEBUG",
    "CRITICAL",
}

REQUIRED_FIELDS = (
    "timestamp",
    "level",
    "service",
    "message",
)

LOG_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} "
    r"\d{2}:\d{2}:\d{2},\d{3}) "
    r"(?P<level>INFO|WARNING|ERROR|DEBUG|CRITICAL) "
    r"(?P<service>[\w.-]+) - "
    r"(?P<message>.*)$"
)

# Tell the framework which format each source uses.
SOURCE_FORMATS = {
    "demo_app": "python_text",
    "payment_api": "json",
}



# ---------------------------------------------------------
# Canonical schema, metadata enrichment, and masking
# ---------------------------------------------------------

SENSITIVE_FIELD_NAMES = {
    "password",
    "passwd",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credit_card",
    "card_number",
    "ssn",
}

EMAIL_PATTERN = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)

# Mask common phone-number-like sequences, avoiding date/time separators.
PHONE_PATTERN = re.compile(
    r"(?<!\w)(?:\+\d{1,3}[\s.-]?)?"
    r"(?:\d[\s.-]?){9,12}\d(?!\w)"
)



TIMESTAMP_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}"
    r"(?:[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{1,9})?"
    r"(?:Z|[+-]\d{2}:?\d{2})?)?\b"
)


def mask_text(value):
    """Mask common PII while preserving timestamp text."""
    if not isinstance(value, str):
        return value

    protected_timestamps = []

    def protect_timestamp(match):
        protected_timestamps.append(match.group(0))
        return f"TIMESTAMPPLACEHOLDER{len(protected_timestamps) - 1}END"

    value = TIMESTAMP_PATTERN.sub(protect_timestamp, value)
    value = EMAIL_PATTERN.sub("[MASKED_EMAIL]", value)
    value = PHONE_PATTERN.sub("[MASKED_PHONE]", value)

    for index, timestamp in enumerate(protected_timestamps):
        value = value.replace(
            f"TIMESTAMPPLACEHOLDER{index}END",
            timestamp,
        )

    return value


def mask_failed_record(raw_record):
    """Mask common sensitive fields in a record that failed parsing."""
    if not isinstance(raw_record, str):
        return raw_record

    # First try the existing structured masking for valid JSON.
    try:
        data = json.loads(raw_record)
        return json.dumps(
            mask_sensitive_data(data),
            ensure_ascii=False,
        )
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    # For malformed JSON or plain text, mask key-value patterns.
    sensitive_keys = (
        r"password|passwd|secret|token|access_token|"
        r"refresh_token|api_key|apikey|authorization|cookie|"
        r"credit_card|card_number|ssn"
    )

    pattern = re.compile(
        rf'(?i)(["\']?(?:{sensitive_keys})["\']?\s*[:=]\s*)'
        rf'("[^"]*"|\'[^\']*\'|[^,\s}}]+)'
    )

    masked = pattern.sub(r'\1"[MASKED]"', raw_record)

    # Also mask common email and phone patterns.
    return mask_text(masked)


def mask_sensitive_data(value, field_name=None):
    """
    Recursively mask sensitive fields and common PII patterns.

    This is a starter implementation, not a complete PII detector.
    """
    normalized_name = str(field_name or "").lower()

    if normalized_name in SENSITIVE_FIELD_NAMES:
        return "[MASKED]"

    if isinstance(value, dict):
        return {
            key: mask_sensitive_data(item, key)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [
            mask_sensitive_data(item, field_name)
            for item in value
        ]

    if isinstance(value, str):
        return mask_text(value)

    return value


def enrich_metadata(source, format_name):
    """Add useful ingestion metadata to a log event."""
    return {
        "source": source,
        "format": format_name,
        "host": socket.gethostname(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def _mask_raw_event(raw_event):
    """Mask sensitive fields in raw JSON, or mask plain-text logs."""
    try:
        parsed = json.loads(raw_event)
    except (json.JSONDecodeError, TypeError):
        return mask_text(raw_event)

    masked = mask_sensitive_data(parsed)

    return json.dumps(masked, ensure_ascii=False)


def build_canonical_record(record, raw_event, source, format_name):
    """
    Build a consistent event structure for downstream processing.

    The raw field stores a masked copy, not an unrestricted original.
    """
    attributes = record.get("attributes", {})

    canonical = {
        "timestamp": record["timestamp"],
        "level": record["level"],
        "service": record["service"],
        "message": record["message"],
        "attributes": attributes,
        "raw": {
            "original_masked": _mask_raw_event(raw_event)
        },
        "metadata": enrich_metadata(source, format_name),
    }

    # Mask the complete event before it leaves the parsing stage.
    return mask_sensitive_data(canonical)



# ---------------------------------------------------------
# Record validation
# ---------------------------------------------------------

def validate_record_detailed(record):
    """
    Validate and normalize a record.

    Returns:
        (normalized_record, None) on success
        (None, error_message) on failure
    """

    if not isinstance(record, dict):
        return None, "Log record must be a JSON object/dictionary"

    # Check required fields, allowing common alternative names
    # to be mapped before this function is called.
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            return None, f"Missing required field: {field}"

    # Check types before calling string methods such as strip().
    for field in REQUIRED_FIELDS:
        if not isinstance(record[field], str):
            return None, f"Field '{field}' must be a string"

    normalized = {
        field: record[field].strip()
        for field in REQUIRED_FIELDS
    }
    normalized["attributes"]=record.get("attributes",{})
    if not isinstance(normalized["attributes"], dict):
        
        return None, "Field 'attributes' must be an object/dictionary"

    normalized["level"] = normalized["level"].upper()

    # Required fields must not be blank.
    for field in REQUIRED_FIELDS:
        if not normalized[field]:
            return None, f"Field '{field}' cannot be empty"

    # Check supported severity levels.
    if normalized["level"] not in VALID_LEVELS:
        return None, f"Invalid severity: {normalized['level']}"

    # Validate the timestamp's actual date and time.
    # Validate and normalize the timestamp.
    timestamp = normalized["timestamp"]

    try:
        # First, try ISO 8601 format.
        # This also supports timestamps ending with Z.
        parsed_timestamp = datetime.fromisoformat(
            timestamp.replace("Z", "+00:00")
        )

    except ValueError:
        try:
            # Handle the demo application's timestamp format.
            parsed_timestamp = datetime.strptime(
                timestamp,
                "%Y-%m-%d %H:%M:%S,%f"
            )

        except ValueError:
            return None, f"Invalid timestamp: {timestamp}"

    # Convert the validated timestamp into ISO 8601 format.
    normalized["timestamp"] = parsed_timestamp.isoformat()
    return normalized,None


def validate_record(record):
    """
    Backward-compatible validator.

    Returns a normalized record if valid, otherwise None.
    Existing parser functions can continue using this function.
    """
    normalized, error = validate_record_detailed(record)
    if error is not None:
        return None
    return normalized


# ---------------------------------------------------------
# Plain-text log parsing
# ---------------------------------------------------------


def parse_log_line(line):
    """Parse and normalize a Python-style plain-text log."""

    if not isinstance(line, str):
        return None

    raw = line.rstrip("\r\n")
    match = LOG_PATTERN.match(raw.strip())

    if match is None:
        return None

    record = validate_record(match.groupdict())

    if record is None:
        return None

    return build_canonical_record(
        record,
        raw,
        source="demo_app",
        format_name="python_text",
    )



# ---------------------------------------------------------
# JSON log parsing
# ---------------------------------------------------------


def _record_from_json_data(data):
    """Map common field names and preserve source-specific fields."""

    if not isinstance(data, dict):
        return None, "JSON log must be an object"

    aliases = {
        "timestamp": ("timestamp", "time"),
        "level": ("level", "severity"),
        "service": ("service", "app"),
        "message": ("message",),
    }

    record = {}

    for field, accepted_names in aliases.items():
        found = next(
            (name for name in accepted_names if name in data),
            None,
        )

        if found is None:
            return None, f"Missing required field: {field}"

        record[field] = data[found]

    # Preserve fields that aren't part of our common schema.
    known_names = {
        name
        for accepted_names in aliases.values()
        for name in accepted_names
    }

    record["attributes"] = {
        key: value
        for key, value in data.items()
        if key not in known_names
    }

    normalized, error = validate_record_detailed(record)

    if error is not None:
        return None, error

    return normalized, None



def parse_json_log_detailed(line):
    """
    Parse JSON and preserve useful error details.

    Returns:
        (normalized_record, None) on success
        (None, error_message) on failure
    """

    if not isinstance(line, str):
        return None, "Log line must be a string"

    try:
        data = json.loads(line)
    except json.JSONDecodeError as exc:
        return None, (
            f"Invalid JSON at line {exc.lineno}, "
            f"column {exc.colno}: {exc.msg}"
        )

    record, error = _record_from_json_data(data)

    if error is not None:
        return None, error

    return build_canonical_record(
        record,
        line.rstrip("\r\n"),
        source="unknown",
        format_name="json",
    ), None

def parse_json_log(line):
    """
    Backward-compatible JSON parser.

    Returns a normalized record if valid, otherwise None.
    """
    record, error = parse_json_log_detailed(line)
    if error is not None:
        return None
    return record


# ---------------------------------------------------------
# General parsing helpers
# ---------------------------------------------------------

def parse_any_log(line):
    """
    Try JSON first, then the supported plain-text format.

    This is useful when the source format is not known in advance.
    """
    parsed_record = parse_json_log(line)

    if parsed_record is not None:
        return parsed_record

    return parse_log_line(line)


def parse_by_format(line, format_name):
    """Parse one line using an explicitly supplied format name."""

    if format_name == "json":
        return parse_json_log(line)

    if format_name == "python_text":
        return parse_log_line(line)

    return None


def parse_by_source(line, source_name):
    """
    Parse a log using the configured source format.

    Return shape:
      Success: {"status": "SUCCESS", "source": ..., "record": ...}
      Failure: {"status": "PARSE_ERROR", "source": ..., "raw": ..., "error": ...}
      Unknown source: {"status": "UNKNOWN_SOURCE", ...}
    """

    raw = line.rstrip("\r\n") if isinstance(line, str) else repr(line)
    format_name = SOURCE_FORMATS.get(source_name)

    if format_name is None:
        return {
            "status": "UNKNOWN_SOURCE",
            "source": source_name,
            "raw": raw,
            "error": f"No format configured for source '{source_name}'",
        }

    if format_name == "json":
        parsed_record, error = parse_json_log_detailed(raw)

        if error is not None:
            return {
                "status": "PARSE_ERROR",
                "source": source_name,
                "raw": raw,
                "error": error,
            }

    elif format_name == "python_text":
        parsed_record = parse_log_line(raw)

        if parsed_record is None:
            return {
                "status": "PARSE_ERROR",
                "source": source_name,
                "raw": raw,
                "error": (
                    "Text log does not match the expected pattern "
                    "or contains invalid field values"
                ),
            }

    else:
        return {
            "status": "PARSE_ERROR",
            "source": source_name,
            "raw": raw,
            "error": f"Unsupported configured format: {format_name}",
        }


    # Ensure metadata reflects the actual configured source.
    parsed_record["metadata"]["source"] = source_name
    parsed_record["metadata"]["format"] = format_name
    
    return {
        "status": "SUCCESS",
        "source": source_name,
        "record": parsed_record,
    }


def process_log_line(line):
    """
    Parse a line when its format is not known.

    Note: this function cannot provide as precise error details as
    parse_by_source(), because it guesses the format.
    """

    parsed_record = parse_any_log(line)

    if parsed_record is None:
        return {
            "status": "PARSE_ERROR",
            "raw": line.rstrip("\r\n") if isinstance(line, str) else repr(line),
            "error": "Unsupported format or malformed log",
        }

    return {
        "status": "SUCCESS",
        "record": parsed_record,
    }


# ---------------------------------------------------------
# Manual demonstration and smoke tests
# ---------------------------------------------------------

def main():
    print("=== Testing plain-text parsing ===")
    demo_line = (
        "2026-10-23 12:40:00,123 "
        "ERROR demo_app - Database failed"
    )
    print(parse_log_line(demo_line))
    print(parse_log_line("This is not a valid log record"))

    print("\n=== Testing JSON parsing ===")
    json_line = (
        '{"timestamp":"2026-10-23T12:35:18Z",'
        '"level":"ERROR",'
        '"service":"payment",'
        '"message":"Payment failed"}'
    )
    print(parse_json_log(json_line))

    print("\n=== Testing alternative JSON field names ===")
    alternative_json = (
        '{"time":"2026-10-23T12:40:00Z",'
        '"severity":"error",'
        '"app":"payment",'
        '"message":"Payment failed"}'
    )
    print(parse_json_log(alternative_json))

    print("\n=== Testing automatic format detection ===")
    print(parse_any_log(json_line))
    print(parse_any_log(demo_line))
    print(parse_any_log("Unknown format"))

    print("\n=== Testing source-based parsing ===")
    payment_line = (
        '{"timestamp":"2026-10-23T12:40:00Z",'
        '"level":"ERROR",'
        '"service":"payment",'
        '"message":"Payment failed"}'
    )
    print(parse_by_source(demo_line, "demo_app"))
    print(parse_by_source(payment_line, "payment_api"))
    print(parse_by_source(demo_line, "unknown_app"))

    print("\n=== Testing invalid records and error messages ===")
    test_cases = [
        (
            "Malformed JSON",
            '{"timestamp":"2026-10-23T12:40:00Z","level":"ERROR"',
        ),
        (
            "Missing field",
            '{"timestamp":"2026-10-23T12:40:00Z",'
            '"level":"ERROR","service":"payment"}',
        ),
        (
            "Invalid severity",
            '{"timestamp":"2026-10-23T12:40:00Z",'
            '"level":"UNKNOWN","service":"payment",'
            '"message":"Payment failed"}',
        ),
        (
            "Empty message",
            '{"timestamp":"2026-10-23T12:40:00Z",'
            '"level":"ERROR","service":"payment","message":"   "}',
        ),
        (
            "Invalid timestamp",
            '{"timestamp":"2026-99-99T12:40:00Z",'
            '"level":"ERROR","service":"payment",'
            '"message":"Payment failed"}',
        ),
        (
            "Wrong timestamp type",
            '{"timestamp":123,"level":"ERROR",'
            '"service":"payment","message":"Payment failed"}',
        ),
        (
            "Wrong severity type",
            '{"timestamp":"2026-10-23T12:40:00Z",'
            '"level":5,"service":"payment",'
            '"message":"Payment failed"}',
        ),
        (
            "JSON array instead of object",
            '["not", "a", "log", "object"]',
        ),
    ]

    for test_name, test_line in test_cases:
        result = parse_by_source(test_line, "payment_api")
        print(f"{test_name}: {result}")

    print("\n=== Testing process_log_line ===")
    print(process_log_line(demo_line))
    print(process_log_line('{"timestamp":'))

    print("\n=== Testing recent lines from demo_app.log ===")
    if not LOG_FILE.exists():
        print("Log file not found:", LOG_FILE)
        print("Run demo_app.py to generate sample logs.")
        return

    try:
        with LOG_FILE.open("r", encoding="utf-8", errors="replace") as log_file:
            lines = log_file.readlines()
    except OSError as exc:
        print(f"Could not read log file: {exc}")
        return

    for line in lines[-5:]:
        print(parse_by_source(line, "demo_app"))


if __name__ == "__main__":
    main()
