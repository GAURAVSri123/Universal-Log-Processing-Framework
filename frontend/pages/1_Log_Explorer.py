
import json
import hashlib

import pandas as pd
import streamlit as st

from elasticsearch_search import ElasticsearchLogSearch


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Log Explorer",
    page_icon="🔎",
    layout="wide"
)

INDEX_NAME = "processed-logs"


# ============================================================
# ELASTICSEARCH CONNECTION
# ============================================================

@st.cache_resource
def get_searcher():
    return ElasticsearchLogSearch()


try:
    searcher = get_searcher()

except Exception as error:
    st.error("Unable to connect to Elasticsearch.")
    st.exception(error)
    st.stop()


# ============================================================
# HEADER
# ============================================================

header_col1, header_col2 = st.columns([5, 1])

with header_col1:
    st.title("🔎 Log Explorer")
    st.caption(
        "Search, filter, analyze, and inspect normalized logs "
        "stored in Elasticsearch."
    )

with header_col2:
    st.write("")
    st.write("")

    if st.button("↻ Refresh", use_container_width=True):
        try:
            searcher.client.indices.refresh(index=INDEX_NAME)
            st.session_state.log_page = 0
            st.rerun()
        except Exception as error:
            st.error(f"Refresh failed: {error}")


# ============================================================
# SESSION STATE
# ============================================================

if "log_page" not in st.session_state:
    st.session_state.log_page = 0

if "log_filter_signature" not in st.session_state:
    st.session_state.log_filter_signature = None


# ============================================================
# DYNAMIC FILTER OPTIONS
# ============================================================

try:
    aggregation_response = searcher.client.search(
        index=INDEX_NAME,
        size=0,
        aggs={
            "services": {
                "terms": {
                    "field": "service",
                    "size": 100
                }
            },
            "classifications": {
                "terms": {
                    "field": "classification",
                    "size": 100
                }
            }
        }
    )

    aggregations = aggregation_response.get("aggregations", {})

    service_options = [
        bucket["key"]
        for bucket in aggregations.get(
            "services", {}
        ).get("buckets", [])
    ]

    classification_options = [
        bucket["key"]
        for bucket in aggregations.get(
            "classifications", {}
        ).get("buckets", [])
    ]

except Exception as error:
    st.error("Unable to retrieve filter options from Elasticsearch.")
    st.exception(error)
    st.stop()


# ============================================================
# SEARCH AND FILTERS
# ============================================================

st.subheader("Search and Filters")

with st.sidebar:
    st.header("🔍 Log Filters")

    if st.button("Reset all filters", use_container_width=True):
        for key in [
            "log_keyword",
            "log_severity",
            "log_priority",
            "log_service",
            "log_classification",
            "log_page_size"
        ]:
            st.session_state.pop(key, None)

        st.session_state.log_page = 0
        st.session_state.log_filter_signature = None
        st.rerun()

    keyword = st.text_input(
        "Search log message",
        placeholder="e.g. timeout, payment, database",
        key="log_keyword"
    )

    severity = st.selectbox(
        "Log Level",
        ["All", "INFO", "WARNING", "ERROR", "CRITICAL"],
        key="log_severity"
    )

    priority = st.selectbox(
        "Priority",
        ["All", "P1", "P2", "P3", "P4", "P5"],
        key="log_priority"
    )

    selected_service = st.selectbox(
        "Service",
        ["All"] + sorted(service_options),
        key="log_service"
    )

    selected_classification = st.selectbox(
        "Classification",
        ["All"] + sorted(classification_options),
        key="log_classification"
    )

    page_size = st.selectbox(
        "Records per page",
        [10, 20, 50, 100],
        index=1,
        key="log_page_size"
    )


# ============================================================
# BUILD ELASTICSEARCH QUERY
# ============================================================

filters = []
must_conditions = []

if severity != "All":
    filters.append({
        "term": {
            "level": severity
        }
    })

if priority != "All":
    filters.append({
        "term": {
            "priority": priority
        }
    })

if selected_service != "All":
    filters.append({
        "term": {
            "service": selected_service
        }
    })

if selected_classification != "All":
    filters.append({
        "term": {
            "classification": selected_classification
        }
    })

if keyword.strip():
    must_conditions.append({
        "match": {
            "message": {
                "query": keyword.strip(),
                "operator": "and"
            }
        }
    })

query = {
    "bool": {
        "filter": filters,
        "must": must_conditions
    }
}


# ============================================================
# RESET PAGINATION WHEN FILTERS CHANGE
# ============================================================

filter_signature = hashlib.md5(
    json.dumps(
        {
            "query": query,
            "page_size": page_size
        },
        sort_keys=True
    ).encode("utf-8")
).hexdigest()

if st.session_state.log_filter_signature != filter_signature:
    st.session_state.log_page = 0
    st.session_state.log_filter_signature = filter_signature


# ============================================================
# COUNT MATCHING RECORDS
# ============================================================

st.divider()

try:
    total_response = searcher.client.count(
        index=INDEX_NAME,
        query=query
    )

    total_records = total_response["count"]

except Exception as error:
    st.error("Unable to count matching logs.")
    st.exception(error)
    st.stop()


# ============================================================
# PAGINATION
# ============================================================

total_pages = max(
    1,
    (total_records + page_size - 1) // page_size
)

if st.session_state.log_page >= total_pages:
    st.session_state.log_page = 0

current_page = st.session_state.log_page
start = current_page * page_size


# ============================================================
# FETCH LOG RECORDS
# ============================================================

try:
    response = searcher.client.search(
        index=INDEX_NAME,
        query=query,
        from_=start,
        size=page_size,
        sort=[
            {
                "timestamp": {
                    "order": "desc",
                    "unmapped_type": "date"
                }
            }
        ]
    )

    logs = []

    for hit in response["hits"]["hits"]:
        record = hit["_source"].copy()
        record["_id"] = hit["_id"]
        logs.append(record)

except Exception as error:
    st.error("Log search failed.")
    st.exception(error)
    st.stop()


# ============================================================
# RESULTS SUMMARY
# ============================================================

st.subheader("📊 Search Results")

error_count = sum(
    1 for log in logs
    if log.get("level") in ["ERROR", "CRITICAL"]
)

affected_services = len({
    log.get("service")
    for log in logs
    if log.get("service")
})

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Matching Logs",
    f"{total_records:,}"
)

col2.metric(
    "Error Logs (Current Page)",
    error_count
)

col3.metric(
    "Affected Services (Current Page)",
    affected_services
)

col4.metric(
    "Current Page",
    f"{current_page + 1} / {total_pages}"
)


# ============================================================
# LOG RESULTS TABLE
# ============================================================

st.divider()
st.subheader("📋 Log Records")

if not logs:

    st.info("No logs match your search criteria.")

else:

    display_rows = []

    for log in logs:

        display_rows.append({
            "Timestamp": log.get("timestamp", "N/A"),
            "Service": log.get("service", "N/A"),
            "Level": log.get("level", "N/A"),
            "Priority": log.get("priority", "N/A"),
            "Classification": log.get(
                "classification", "N/A"
            ),
            "Message": log.get("message", "N/A")
        })

    df = pd.DataFrame(display_rows)

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=420
    )


    # ========================================================
    # COMPLETE LOG INSPECTION
    # ========================================================

    st.divider()
    st.subheader("🔍 Inspect Log Details")

    log_options = {}

    for index, log in enumerate(logs):

        label = (
            f"{log.get('timestamp', 'N/A')} | "
            f"{log.get('service', 'N/A')} | "
            f"{log.get('level', 'N/A')} | "
            f"Record {index + 1}"
        )

        log_options[label] = log

    selected_log_label = st.selectbox(
        "Select a log record to inspect",
        list(log_options.keys())
    )

    selected_log = log_options[selected_log_label]

    # Log information cards
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Service",
        selected_log.get("service", "N/A")
    )

    col2.metric(
        "Log Level",
        selected_log.get("level", "N/A")
    )

    col3.metric(
        "Priority",
        selected_log.get("priority", "N/A")
    )

    col4.metric(
        "Classification",
        selected_log.get("classification", "N/A")
    )

    st.markdown("**Log Message**")

    st.info(
        selected_log.get(
            "message",
            "No message available"
        )
    )

    with st.expander("View complete log record", expanded=True):
        st.json(selected_log)


# ============================================================
# PAGINATION CONTROLS
# ============================================================

st.divider()

col1, col2, col3 = st.columns([1, 2, 1])

with col1:

    if st.button(
        "← Previous",
        disabled=current_page == 0,
        use_container_width=True
    ):
        st.session_state.log_page -= 1
        st.rerun()

with col2:

    if total_records > 0:
        first_record = start + 1
        last_record = min(
            start + len(logs),
            total_records
        )

        st.caption(
            f"Showing {first_record}–{last_record} "
            f"of {total_records:,} matching records"
        )
    else:
        st.caption("No matching records")

with col3:

    if st.button(
        "Next →",
        disabled=current_page >= total_pages - 1,
        use_container_width=True
    ):
        st.session_state.log_page += 1
        st.rerun()


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Log Explorer | Elasticsearch Index: processed-logs"
)
