"""Focused tests for XML, CSV, Syslog, CEF, and LEEF log formats."""

from log_parser import (
    parse_by_format,
    parse_by_source,
)


def assert_success(name, result, expected_format, expected_level):
    assert result is not None, f"{name}: parser returned None"
    assert result["metadata"]["format"] == expected_format, (
        f"{name}: wrong format metadata: {result['metadata']}"
    )
    assert result["level"] == expected_level, (
        f"{name}: expected {expected_level}, got {result['level']}"
    )
    assert result["timestamp"], f"{name}: timestamp missing"
    assert result["service"], f"{name}: service missing"
    assert result["message"], f"{name}: message missing"
    print(f"[PASS] {name}")


def main():
    xml_line = (
        '<event timestamp="2026-11-10T10:00:00Z" level="ERROR" '
        'service="checkout" message="Payment failed"><code>PAY-01</code></event>'
    )
    xml = parse_by_format(xml_line, "xml", "xml_app")
    assert_success("XML attributes and child fields", xml, "xml", "ERROR")
    assert xml["attributes"]["code"] == "PAY-01"

    csv_line = (
        '2026-11-10T10:01:00Z,WARNING,checkout,'
        '"Payment latency is high, retrying",region-west'
    )
    csv_record = parse_by_format(csv_line, "csv", "csv_app")
    assert_success("CSV quoted message and extra column", csv_record, "csv", "WARNING")
    assert csv_record["message"] == "Payment latency is high, retrying"
    assert csv_record["attributes"]["field_5"] == "region-west"

    syslog_5424 = (
        '<34>1 2026-11-10T10:02:00Z host01 sshd 1234 ID47 '
        '[exampleSDID@32473 iut="3"] Failed password for user'
    )
    syslog = parse_by_format(syslog_5424, "syslog", "syslog_app")
    assert_success("Syslog RFC 5424", syslog, "syslog", "CRITICAL")
    assert syslog["attributes"]["host"] == "host01"

    syslog_3164 = (
        '<11>Nov 10 10:03:00 host01 sshd[1234]: Authentication failed'
    )
    syslog_old = parse_by_format(syslog_3164, "syslog", "syslog_app")
    assert_success("Syslog RFC 3164", syslog_old, "syslog", "ERROR")
    assert syslog_old["attributes"]["timestamp_year_inferred"] is True

    cef_line = (
        'CEF:0|Acme|Endpoint|2.1|1001|Malware detected|9|'
        'rt=1794305040000 src=10.0.0.5 msg=Suspicious process detected'
    )
    cef = parse_by_format(cef_line, "cef", "cef_app")
    assert_success("CEF with epoch-millisecond timestamp", cef, "cef", "CRITICAL")
    assert cef["attributes"]["src"] == "10.0.0.5"

    leef_tab = (
        'LEEF:1.0|Acme|Endpoint|2.1|1001|'
        'devTime=2026-11-10T10:05:00Z\tsev=8\tcat=Malware\t'
        'src=10.0.0.6\tmsg=Suspicious process'
    )
    leef = parse_by_format(leef_tab, "leef", "leef_app")
    assert_success("LEEF 1.0 tab-delimited extension", leef, "leef", "ERROR")
    assert leef["attributes"]["src"] == "10.0.0.6"

    leef_caret = (
        'LEEF:2.0|Acme|Firewall|3.0|2002|^|'
        'devTimeEpoch=1794305160000^sev=10^src=10.0.0.7^msg=Blocked attack'
    )
    leef2 = parse_by_format(leef_caret, "leef", "leef_app")
    assert_success("LEEF 2.0 custom delimiter", leef2, "leef", "CRITICAL")
    assert leef2["attributes"]["src"] == "10.0.0.7"

    invalid_cases = [
        ("Invalid XML", "<event><timestamp>bad</event>", "xml"),
        ("Invalid CSV", "not,enough,columns", "csv"),
        ("Invalid Syslog", "not a syslog message", "syslog"),
        (
            "Syslog priority outside RFC range",
            '<999>1 2026-11-10T10:02:00Z host01 sshd 1234 ID47 - Invalid priority',
            "syslog",
        ),
        ("Invalid CEF", "CEF:0|too|few", "cef"),
        ("Invalid LEEF", "LEEF:2.0|too|few", "leef"),
    ]

    for name, raw, fmt in invalid_cases:
        result = parse_by_format(raw, fmt, "format_test")
        assert result is None, f"{name}: invalid input unexpectedly parsed"
        print(f"[PASS] {name} rejected")

    # Ensure all five formats work through the collector's source routing.
    source_routes = [
        ("XML", xml_line, "xml_app"),
        ("CSV", csv_line, "csv_app"),
        ("Syslog", syslog_5424, "syslog_app"),
        ("CEF", cef_line, "cef_app"),
        ("LEEF", leef_tab, "leef_app"),
    ]
    for name, raw, source_name in source_routes:
        routed = parse_by_source(raw, source_name)
        assert routed["status"] == "SUCCESS", (name, routed)
        assert routed["record"]["metadata"]["source"] == source_name
        print(f"[PASS] Source-based {name} routing")

    print("\nAll format parser checks passed.")


if __name__ == "__main__":
    main()
