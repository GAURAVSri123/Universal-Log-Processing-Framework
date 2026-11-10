import re
import json
import csv
import io
import xml.etree.ElementTree as ET
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
    "app": "simple_text",
    "server": "simple_text",
    "collector_rotation_test": "python_text",
    "xml_app": "xml",
    "csv_app": "csv",
    "syslog_app": "syslog",
    "cef_app": "cef",
    "leef_app": "leef",
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
# Simple timestamp-level-message text parsing
# ---------------------------------------------------------

SIMPLE_TEXT_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} "
    r"\d{2}:\d{2}:\d{2}(?:[.,]\d{1,6})?)"
    r"\s+(?P<level>INFO|WARNING|ERROR|DEBUG|CRITICAL)"
    r"\s+(?P<message>.+)$"
)


def parse_simple_text_log_detailed(line, source_name):
    """
    Parse text logs shaped like:
    2026-10-22 17:00:01 INFO Application started

    The source filename supplies the service name because this format
    does not contain an explicit service field.
    """
    if not isinstance(line, str):
        return None, "Log line must be a string"

    raw = line.rstrip("\r\n")
    match = SIMPLE_TEXT_PATTERN.match(raw.strip())

    if match is None:
        return None, (
            "Text log does not match the expected "
            "timestamp-level-message pattern"
        )

    record = {
        "timestamp": match.group("timestamp"),
        "level": match.group("level"),
        "service": source_name,
        "message": match.group("message").strip(),
        "attributes": {},
    }

    normalized, error = validate_record_detailed(record)
    if error is not None:
        return None, error

    return build_canonical_record(
        normalized,
        raw,
        source=source_name,
        format_name="simple_text",
    ), None


# ---------------------------------------------------------
# Structured and security-event format parsers
# ---------------------------------------------------------


def _canonicalize_parsed_record(record, raw, source_name, format_name):
    """Validate a parsed record and build the shared canonical schema."""
    normalized, error = validate_record_detailed(record)
    if error is not None:
        return None, error

    return build_canonical_record(
        normalized,
        raw,
        source=source_name,
        format_name=format_name,
    ), None


def _parse_event_timestamp(value):
    """Normalize ISO timestamps and Unix epoch timestamps."""
    if value is None:
        return None

    value = str(value).strip()
    if not value or value == "-":
        return None

    try:
        # Support epoch seconds and epoch milliseconds.
        if re.fullmatch(r"\d{10}(?:\d{3})?", value):
            epoch = int(value)
            if len(value) == 13:
                epoch = epoch / 1000
            return datetime.fromtimestamp(
                epoch, tz=timezone.utc
            ).isoformat()

        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
            return parsed.isoformat()
        except ValueError:
            # Common CEF device-event timestamps.
            for timestamp_format in (
                "%b %d %Y %H:%M:%S",
                "%b %d %Y %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
            ):
                try:
                    return datetime.strptime(
                        value, timestamp_format
                    ).isoformat()
                except ValueError:
                    continue
            return None
    except (ValueError, OverflowError, OSError):
        return None


def parse_xml_log_detailed(line, source_name):
    """Parse one-line XML log records with attributes or child elements."""
    if not isinstance(line, str):
        return None, "XML log line must be a string"

    try:
        root = ET.fromstring(line)
    except ET.ParseError as exc:
        return None, f"Invalid XML: {exc}"

    data = {str(key).lower(): value for key, value in root.attrib.items()}
    for child in root:
        tag = child.tag.split("}")[-1].lower()
        data[tag] = (child.text or "").strip()

    aliases = {
        "timestamp": ("timestamp", "time", "datetime"),
        "level": ("level", "severity"),
        "service": ("service", "app", "application"),
        "message": ("message", "msg"),
    }

    record = {}
    for target, names in aliases.items():
        found = next((data[name] for name in names if name in data), None)
        if found is None and target == "service":
            found = source_name
        if found is None:
            return None, f"XML record is missing required field: {target}"
        record[target] = found

    known = {name for names in aliases.values() for name in names}
    record["attributes"] = {
        key: value for key, value in data.items() if key not in known
    }
    return _canonicalize_parsed_record(
        record, line, source_name, "xml"
    )


def parse_csv_log_detailed(line, source_name):
    """
    Parse a headerless CSV event row in this fixed order:
    timestamp,level,service,message

    Additional columns are retained as field_5, field_6, etc.
    The collector processes one physical line at a time, so CSV headers
    and multiline quoted fields are intentionally not supported.
    """
    if not isinstance(line, str):
        return None, "CSV log line must be a string"

    try:
        rows = list(csv.reader(io.StringIO(line), strict=True))
    except csv.Error as exc:
        return None, f"Invalid CSV: {exc}"

    if len(rows) != 1 or len(rows[0]) < 4:
        return None, (
            "CSV record must be one line with at least four columns: "
            "timestamp,level,service,message"
        )

    values = rows[0]
    if [value.strip().lower() for value in values[:4]] == [
        "timestamp", "level", "service", "message"
    ]:
        return None, (
            "CSV header rows are not events; use headerless rows in the "
            "documented timestamp,level,service,message column order"
        )

    record = {
        "timestamp": values[0],
        "level": values[1],
        "service": values[2] or source_name,
        "message": values[3],
        "attributes": {
            f"field_{index + 1}": value
            for index, value in enumerate(values[4:], start=4)
        },
    }
    return _canonicalize_parsed_record(
        record, line, source_name, "csv"
    )


SYSLOG_SEVERITY = {
    0: "CRITICAL", 1: "CRITICAL", 2: "CRITICAL",
    3: "ERROR", 4: "WARNING", 5: "INFO",
    6: "INFO", 7: "DEBUG",
}


def _syslog_level(priority):
    severity_code = int(priority) % 8
    return SYSLOG_SEVERITY[severity_code]


def parse_syslog_log_detailed(line, source_name):
    """Parse common RFC 5424 and RFC 3164 syslog message forms."""
    if not isinstance(line, str):
        return None, "Syslog record must be a string"

    raw = line.strip()

    # RFC 5424: PRI VERSION TIMESTAMP HOST APP PROCID MSGID STRUCTURED-DATA MSG
    rfc5424 = re.match(
        r"^<(?P<pri>\d{1,3})>1\s+"
        r"(?P<timestamp>\S+)\s+(?P<host>\S+)\s+"
        r"(?P<app>\S+)\s+(?P<procid>\S+)\s+"
        r"(?P<msgid>\S+)\s+(?P<structured>(?:\[.*?\]|-))"
        r"(?:\s+(?P<message>.*))?$",
        raw,
    )
    if rfc5424:
        fields = rfc5424.groupdict()
        timestamp = _parse_event_timestamp(fields["timestamp"])
        if timestamp is None:
            return None, "RFC 5424 syslog record has an invalid timestamp"
        record = {
            "timestamp": timestamp,
            "level": _syslog_level(fields["pri"]),
            "service": fields["app"],
            "message": fields.get("message") or fields["msgid"],
            "attributes": {
                "host": fields["host"],
                "process_id": fields["procid"],
                "message_id": fields["msgid"],
                "structured_data": fields["structured"],
                "priority": int(fields["pri"]),
            },
        }
        return _canonicalize_parsed_record(
            record, line, source_name, "syslog"
        )

    # RFC 3164: <PRI>Mmm dd HH:MM:SS HOST TAG[PID]: MESSAGE
    rfc3164 = re.match(
        r"^<(?P<pri>\d{1,3})>"
        r"(?P<month>[A-Z][a-z]{2})\s+"
        r"(?P<day>\d{1,2})\s+"
        r"(?P<clock>\d{2}:\d{2}:\d{2})\s+"
        r"(?P<host>\S+)\s+(?P<tag>[^:\s\[]+)"
        r"(?:\[(?P<pid>\d+)\])?:\s*(?P<message>.*)$",
        raw,
    )
    if rfc3164:
        fields = rfc3164.groupdict()
        try:
            parsed = datetime.strptime(
                f"{datetime.now().year} {fields['month']} "
                f"{fields['day']} {fields['clock']}",
                "%Y %b %d %H:%M:%S",
            )
        except ValueError as exc:
            return None, f"Invalid RFC 3164 syslog timestamp: {exc}"

        record = {
            "timestamp": parsed.isoformat(),
            "level": _syslog_level(fields["pri"]),
            "service": fields["tag"],
            "message": fields["message"],
            "attributes": {
                "host": fields["host"],
                "process_id": fields["pid"],
                "priority": int(fields["pri"]),
                "timestamp_year_inferred": True,
            },
        }
        return _canonicalize_parsed_record(
            record, line, source_name, "syslog"
        )

    return None, "Syslog record does not match RFC 5424 or RFC 3164"


def _split_escaped_pipe(value):
    """Split CEF/LEEF headers on unescaped pipe characters."""
    parts = []
    current = []
    escaped = False

    for char in value:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "|":
            parts.append("".join(current))
            current = []
        else:
            current.append(char)

    if escaped:
        current.append("\\")
    parts.append("".join(current))
    return parts


def _parse_key_value_extension(extension, delimiter_pattern=r"\s+"):
    """Parse key=value extension fields while allowing spaces in values."""
    matches = list(re.finditer(
        r"(?:^|" + delimiter_pattern + r")([A-Za-z][A-Za-z0-9]*)=",
        extension,
    ))
    result = {}
    for index, match in enumerate(matches):
        value_start = match.end()
        value_end = (
            matches[index + 1].start()
            if index + 1 < len(matches)
            else len(extension)
        )
        result[match.group(1)] = extension[value_start:value_end].strip()
    return result


def _cef_severity(value):
    try:
        severity = int(value)
    except (TypeError, ValueError):
        return None
    if not 0 <= severity <= 10:
        return None
    if severity <= 3:
        return "INFO"
    if severity <= 6:
        return "WARNING"
    if severity <= 8:
        return "ERROR"
    return "CRITICAL"


def parse_cef_log_detailed(line, source_name):
    """Parse a Common Event Format (CEF) security event."""
    if not isinstance(line, str) or not line.startswith("CEF:"):
        return None, "CEF record must begin with CEF:"

    parts = _split_escaped_pipe(line.rstrip("\r\n"))
    if len(parts) < 8:
        return None, "CEF record must contain a seven-field header and extension"

    version = parts[0][4:]
    vendor, product, device_version, signature_id, name, severity = parts[1:7]
    extension = "|".join(parts[7:])
    fields = _parse_key_value_extension(extension)

    timestamp_value = (
        fields.get("rt") or fields.get("start") or fields.get("end")
    )
    timestamp = _parse_event_timestamp(timestamp_value)
    if timestamp is None:
        return None, "CEF extension must include a valid rt, start, or end timestamp"

    level = _cef_severity(severity)
    if level is None:
        return None, "CEF severity must be an integer from 0 to 10"

    record = {
        "timestamp": timestamp,
        "level": level,
        "service": product or vendor or source_name,
        "message": name or signature_id,
        "attributes": {
            "cef_version": version,
            "device_vendor": vendor,
            "device_product": product,
            "device_version": device_version,
            "signature_id": signature_id,
            "cef_severity": severity,
            **fields,
        },
    }
    return _canonicalize_parsed_record(
        record, line, source_name, "cef"
    )


def _leef_severity(value):
    try:
        severity = int(value)
    except (TypeError, ValueError):
        return None
    if not 0 <= severity <= 10:
        return None
    if severity <= 3:
        return "INFO"
    if severity <= 6:
        return "WARNING"
    if severity <= 8:
        return "ERROR"
    return "CRITICAL"


def parse_leef_log_detailed(line, source_name):
    """Parse LEEF 1.0/2.0 records with tab-delimited extensions."""
    if not isinstance(line, str) or not line.startswith("LEEF:"):
        return None, "LEEF record must begin with LEEF:"

    # LEEF 1.0 has five header separators; LEEF 2.0 may add a
    # sixth field declaring the extension delimiter.
    parts = line.rstrip("\r\n").split("|", 6)
    if len(parts) < 6:
        return None, "LEEF record is missing its extension separator"

    leef_version = parts[0][5:]
    vendor, product, device_version, event_id = parts[1:5]

    if len(parts) == 6:
        extension_delimiter = "\t"
        extension = parts[5]
    else:
        delimiter_name = parts[5]
        extension_delimiter = (
            "\t"
            if delimiter_name.lower() in ("tab", "\\t")
            else delimiter_name
        )
        extension = parts[6]

    if extension_delimiter == "\t":
        fields = _parse_key_value_extension(extension, delimiter_pattern=r"\t+")
    else:
        fields = _parse_key_value_extension(
            extension,
            delimiter_pattern=re.escape(extension_delimiter) + r"+",
        )

    timestamp_value = fields.get("devTime") or fields.get("devTimeEpoch")
    timestamp = _parse_event_timestamp(timestamp_value)
    if timestamp is None:
        return None, "LEEF extension must include a valid devTime or devTimeEpoch"

    level = _leef_severity(fields.get("sev", "5"))
    if level is None:
        return None, "LEEF severity (sev) must be an integer from 0 to 10"

    record = {
        "timestamp": timestamp,
        "level": level,
        "service": product or vendor or source_name,
        "message": fields.get("msg") or fields.get("cat") or event_id,
        "attributes": {
            "leef_version": leef_version,
            "device_vendor": vendor,
            "device_product": product,
            "device_version": device_version,
            "event_id": event_id,
            **fields,
        },
    }
    return _canonicalize_parsed_record(
        record, line, source_name, "leef"
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


def parse_by_format(line, format_name, source_name="unknown"):
    """Parse one line using an explicitly supplied format name."""

    if format_name == "json":
        return parse_json_log(line)

    if format_name == "python_text":
        return parse_log_line(line)

    if format_name == "simple_text":
        record, error = parse_simple_text_log_detailed(line, source_name)
        return record if error is None else None

    detailed_parsers = {
        "xml": parse_xml_log_detailed,
        "csv": parse_csv_log_detailed,
        "syslog": parse_syslog_log_detailed,
        "cef": parse_cef_log_detailed,
        "leef": parse_leef_log_detailed,
    }
    parser = detailed_parsers.get(format_name)
    if parser is not None:
        record, error = parser(line, source_name)
        return record if error is None else None

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

    elif format_name in {"xml", "csv", "syslog", "cef", "leef"}:
        parser = {
            "xml": parse_xml_log_detailed,
            "csv": parse_csv_log_detailed,
            "syslog": parse_syslog_log_detailed,
            "cef": parse_cef_log_detailed,
            "leef": parse_leef_log_detailed,
        }[format_name]
        parsed_record, error = parser(raw, source_name)

        if error is not None:
            return {
                "status": "PARSE_ERROR",
                "source": source_name,
                "raw": raw,
                "error": error,
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
