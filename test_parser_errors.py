from log_parser import parse_by_source
import json


tests = [
    (
        "Unknown source",
        "2026-10-23 20:00:00,000 INFO server - Server started",
        "unknown_service",
        "UNKNOWN_SOURCE",
    ),
    (
        "Malformed JSON",
        '{"timestamp": "2026-10-23 20:00:00,000", "level": "ERROR"',
        "payment_api",
        "PARSE_ERROR",
    ),
    (
        "Missing required fields",
        json.dumps({
            "level": "ERROR",
            "message": "Payment failed",
        }),
        "payment_api",
        "PARSE_ERROR",
    ),
    (
        "Invalid severity",
        json.dumps({
            "timestamp": "2026-10-23 20:00:00,000",
            "level": "URGENT",
            "service": "payment_api",
            "message": "Payment failed",
        }),
        "payment_api",
        "PARSE_ERROR",
    ),
    (
        "Valid simple-text server log",
        "2026-10-22 10:16:01 INFO Server started",
        "server",
        "SUCCESS",
    ),
    (
        "Valid simple-text app log",
        "2026-10-22 17:00:02 ERROR Database connection failed",
        "app",
        "SUCCESS",
    ),
    (
        "Valid JSON record",
        json.dumps({
            "timestamp": "2026-10-23 20:00:00,000",
            "level": "ERROR",
            "service": "payment_api",
            "message": "Payment failed",
        }),
        "payment_api",
        "SUCCESS",
    ),
]


for test_name, line, source_name, expected_status in tests:
    result = parse_by_source(line, source_name)
    actual_status = result["status"]

    assert actual_status == expected_status, (
        f"{test_name} failed: expected {expected_status}, "
        f"received {actual_status}. Result: {result}"
    )

    print(f"[PASS] {test_name}: {actual_status}")

print(f"\nAll {len(tests)} parser tests passed.")
