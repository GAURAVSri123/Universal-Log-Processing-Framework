
from log_parser import parse_by_source
import json

tests = [
    (
        "Unknown source",
        "2026-10-23 20:00:00,000 INFO server - Server started",
        "server"
    ),
    (
        "Malformed JSON",
        '{"timestamp": "2026-10-23 20:00:00,000", "level": "ERROR"',
        "payment_api"
    ),
    (
        "Missing required fields",
        json.dumps({
            "level": "ERROR",
            "message": "Payment failed"
        }),
        "payment_api"
    ),
    (
        "Invalid severity",
        json.dumps({
            "timestamp": "2026-10-23 20:00:00,000",
            "level": "URGENT",
            "service": "payment_api",
            "message": "Payment failed"
        }),
        "payment_api"
    ),
    (
        "Valid JSON record",
        json.dumps({
            "timestamp": "2026-10-23 20:00:00,000",
            "level": "ERROR",
            "service": "payment_api",
            "message": "Payment failed"
        }),
        "payment_api"
    )
]

for test_name, line, source_name in tests:
    result = parse_by_source(line, source_name)

    print(f"\nTEST: {test_name}")
    print(f"STATUS: {result['status']}")
    print(f"RESULT: {result}")
