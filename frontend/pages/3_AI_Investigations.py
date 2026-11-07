import streamlit as st
import pandas as pd

from frontend.investigations_service import InvestigationsService


st.set_page_config(
    page_title="AI Investigations",
    page_icon="🔎",
    layout="wide",
)


@st.cache_resource
def get_investigations_service():
    return InvestigationsService()


def value_or(value, fallback="Not available"):
    return value if value not in (None, "", [], {}) else fallback


def get_severity(event):
    diagnosis = event.get("diagnosis") or {}
    return str(diagnosis.get("severity") or event.get("log", {}).get("priority") or "UNKNOWN").upper()


def get_service(event):
    return value_or((event.get("log") or {}).get("service"), "Unknown service")


def get_timestamp(event):
    return event.get("analyzed_at") or (event.get("log") or {}).get("timestamp") or "Unknown time"


def severity_rank(severity):
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(severity, 4)


st.title("🔎 AI Investigations")
st.caption("Review AI-generated diagnoses from the existing Kafka → RAG → Groq pipeline.")

# A manual refresh avoids repeatedly replaying the retained Kafka topic on every rerun.
header_left, header_right = st.columns([5, 1])
with header_right:
    refresh_clicked = st.button("↻ Refresh", use_container_width=True)

if "investigation_records" not in st.session_state:
    st.session_state.investigation_records = None
if "investigation_errors" not in st.session_state:
    st.session_state.investigation_errors = []

if st.session_state.investigation_records is None or refresh_clicked:
    with st.spinner("Loading diagnosis events from Kafka..."):
        service = get_investigations_service()
        records, errors = service.fetch_investigations()
        st.session_state.investigation_records = records
        st.session_state.investigation_errors = errors

records = st.session_state.investigation_records or []
errors = st.session_state.investigation_errors or []

if errors:
    with st.expander("Connection / data warnings", expanded=not records):
        for error in errors:
            st.warning(error)

if not records:
    st.info(
        "No diagnosis events were found in `ai-diagnosis`. Start Kafka and "
        "`ai_diagnosis_service.py`, generate an ERROR or CRITICAL log, then select Refresh."
    )
    st.stop()

# Sidebar filters
with st.sidebar:
    st.header("Investigation filters")
    services = sorted({get_service(item) for item in records})
    severities = sorted({get_severity(item) for item in records}, key=severity_rank)
    selected_services = st.multiselect("Services", services, default=services)
    selected_severities = st.multiselect("Severity", severities, default=severities)
    search_text = st.text_input("Search message or diagnosis", placeholder="e.g. database timeout")

filtered = []
for event in records:
    if get_service(event) not in selected_services:
        continue
    if get_severity(event) not in selected_severities:
        continue
    log_data = event.get("log") or {}
    diagnosis = event.get("diagnosis") or {}
    searchable = " ".join([
        str(log_data.get("message", "")),
        str(diagnosis.get("problem_summary", "")),
        str(get_service(event)),
    ]).lower()
    if search_text and search_text.lower() not in searchable:
        continue
    filtered.append(event)

# KPI cards
critical_count = sum(get_severity(item) == "CRITICAL" for item in filtered)
high_count = sum(get_severity(item) == "HIGH" for item in filtered)
affected_services = len({get_service(item) for item in filtered})
latest_time = get_timestamp(filtered[0]) if filtered else "—"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total investigations", len(filtered))
k2.metric("Critical investigations", critical_count)
k3.metric("High severity", high_count)
k4.metric("Affected services", affected_services)

st.divider()
st.subheader("Investigation records")
st.caption(f"Showing {len(filtered)} of {len(records)} retained diagnosis events. Latest event: {latest_time}")

if not filtered:
    st.warning("No investigations match the selected filters.")
    st.stop()

# Compact overview table
summary_rows = []
for event in filtered:
    log_data = event.get("log") or {}
    diagnosis = event.get("diagnosis") or {}
    summary_rows.append({
        "Time": get_timestamp(event),
        "Service": get_service(event),
        "Log level": value_or(log_data.get("level")),
        "Severity": get_severity(event),
        "Priority": value_or(log_data.get("priority")),
        "Status": value_or(event.get("diagnosis_status")),
        "Summary": value_or(diagnosis.get("problem_summary")),
    })

summary_df = pd.DataFrame(summary_rows)
st.dataframe(summary_df, use_container_width=True, hide_index=True)

st.divider()
st.subheader("Diagnosis details")
for index, event in enumerate(filtered):
    log_data = event.get("log") or {}
    diagnosis = event.get("diagnosis") or {}
    severity = get_severity(event)
    title = (
        f"{severity} · {get_service(event)} · "
        f"{value_or(diagnosis.get('problem_summary'), log_data.get('message', 'Investigation'))}"
    )

    with st.expander(title, expanded=(index == 0)):
        top1, top2, top3 = st.columns(3)
        top1.write(f"**Analyzed at:** {get_timestamp(event)}")
        top2.write(f"**Log level:** {value_or(log_data.get('level'))}")
        top3.write(f"**Priority:** {value_or(log_data.get('priority'))}")

        st.markdown("**Original log message**")
        st.info(value_or(log_data.get("message")))

        st.markdown("**Problem summary**")
        st.write(value_or(diagnosis.get("problem_summary")))

        causes = diagnosis.get("possible_causes") or []
        st.markdown("**Possible causes and verification steps**")
        if causes:
            for cause_index, cause in enumerate(causes, start=1):
                if not isinstance(cause, dict):
                    st.write(f"{cause_index}. {cause}")
                    continue
                st.markdown(f"**{cause_index}. {value_or(cause.get('cause'))}**")
                st.write(f"- Supporting evidence: {value_or(cause.get('supporting_evidence'))}")
                st.write(f"- How to verify: {value_or(cause.get('verification_steps'))}")
        else:
            st.write("No possible causes were returned by the diagnosis model.")

        confirmed = diagnosis.get("confirmed_causes") or []
        st.markdown("**Confirmed causes**")
        if confirmed:
            for cause in confirmed:
                st.write(f"- {cause}")
        else:
            st.write("No cause is marked as confirmed. Treat possible causes as hypotheses until verified.")

        actions = diagnosis.get("recommended_actions") or []
        st.markdown("**Recommended actions**")
        if actions:
            for action in actions:
                st.write(f"- {action}")
        else:
            st.write("No recommended actions were returned.")

        bottom1, bottom2 = st.columns(2)
        with bottom1:
            st.markdown("**AI confidence**")
            st.write(value_or(diagnosis.get("confidence")))
        with bottom2:
            st.markdown("**Limitations**")
            st.write(value_or(diagnosis.get("limitations")))

        with st.expander("Event metadata"):
            st.json({
                "event_id": event.get("event_id"),
                "diagnosis_status": event.get("diagnosis_status"),
                "source": event.get("source"),
                "kafka_location": event.get("_kafka"),
            })

st.caption(
    "Data source: ai-diagnosis Kafka topic. This page displays the diagnosis event "
    "published by the existing AI diagnosis service; it does not independently "
    "re-run diagnosis or claim that a historical event describes the current system state."
)
