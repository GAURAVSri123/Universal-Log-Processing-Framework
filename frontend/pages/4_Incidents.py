import pandas as pd
import streamlit as st

from frontend.incidents_service import IncidentsService

st.set_page_config(page_title="Incidents", page_icon="🚨", layout="wide")


@st.cache_resource
def get_incidents_service():
    return IncidentsService()


st.title("🚨 Incident Center")
st.caption("Review incident candidates derived from the existing AI diagnosis events.")

service = get_incidents_service()
@st.cache_data(ttl=15, show_spinner=False)
def load_incidents():
    return service.fetch_incidents(limit=200)

header_left, header_right = st.columns([5, 1])
with header_right:
    if st.button("↻ Refresh", use_container_width=True):
        load_incidents.clear()
        st.rerun()

try:
    with st.spinner("Loading incident records from Kafka..."):
        incidents, errors = load_incidents()
except Exception as exc:
    incidents, errors = [], [str(exc)]

if errors:
    with st.expander("Data-source messages", expanded=not incidents):
        for error in errors:
            st.warning(error)

if not incidents:
    st.info("No diagnosis events are available yet. Check that Kafka is running and that ai_diagnosis_service.py has published events to the ai-diagnosis topic.")
    st.stop()

# Severity is taken directly from the diagnosis event. It is not recalculated here.
def severity_rank(value):
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(value, 4)

incidents.sort(key=lambda item: (severity_rank(item["severity"]), item["timestamp"]), reverse=False)

critical_count = sum(item["severity"] == "CRITICAL" for item in incidents)
high_count = sum(item["severity"] == "HIGH" for item in incidents)
services_count = len({item["service"] for item in incidents})
completed_count = sum(item["diagnosis_status"] == "COMPLETED" for item in incidents)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total incident records", len(incidents))
k2.metric("Critical severity", critical_count)
k3.metric("High severity", high_count)
k4.metric("Affected services", services_count)

st.divider()
st.subheader("Incident records")
st.caption(f"Showing up to {len(incidents)} retained AI diagnosis events. {completed_count} have diagnosis status COMPLETED. Diagnosis completion does not mean the underlying incident is resolved.")

services = sorted({item["service"] for item in incidents})
severities = sorted({item["severity"] for item in incidents})
levels = sorted({item["log_level"] for item in incidents})

with st.sidebar:
    st.header("Incident filters")
    selected_services = st.multiselect("Services", services, default=services)
    selected_severities = st.multiselect("Severity", severities, default=severities)
    selected_levels = st.multiselect("Log level", levels, default=levels)
    search_text = st.text_input("Search message or diagnosis", placeholder="e.g. database timeout")

filtered = []
needle = search_text.strip().lower()
for item in incidents:
    if item["service"] not in selected_services:
        continue
    if item["severity"] not in selected_severities:
        continue
    if item["log_level"] not in selected_levels:
        continue
    searchable = " ".join([
        item["message"], item["summary"], item["classification"], item["service"],
        str(item["possible_causes"]), str(item["recommended_actions"])
    ]).lower()
    if needle and needle not in searchable:
        continue
    filtered.append(item)

st.write(f"**{len(filtered)}** matching incident records")

if not filtered:
    st.info("No incident records match the selected filters.")
else:
    table_rows = [{
        "Time": item["timestamp"],
        "Service": item["service"],
        "Severity": item["severity"],
        "Log level": item["log_level"],
        "Priority": item["priority"],
        "Diagnosis": item["diagnosis_status"],
        "Summary": item["summary"],
    } for item in filtered]
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    st.subheader("Incident details")
    for index, item in enumerate(filtered):
        label = f"{item['severity']} · {item['service']} · {item['timestamp']}"
        with st.expander(label, expanded=(index == 0)):
            st.markdown(f"**Incident reference:** `{item['incident_id']}`")
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Log level:** {item['log_level']}")
            c2.write(f"**Priority:** {item['priority']}")
            c3.write(f"**Confidence:** {item['confidence']}")
            st.markdown("**Original log message**")
            st.code(item["message"], language=None)
            st.markdown("**AI problem summary**")
            st.write(item["summary"])

            causes = item["possible_causes"]
            st.markdown("**Possible causes**")
            if causes:
                for cause_index, cause in enumerate(causes, start=1):
                    if isinstance(cause, dict):
                        st.markdown(f"{cause_index}. **{cause.get('cause', 'Unspecified cause')}**")
                        if cause.get("supporting_evidence"):
                            st.write(f"Supporting evidence: {cause['supporting_evidence']}")
                        if cause.get("verification_steps"):
                            st.write(f"Verification: {cause['verification_steps']}")
                    else:
                        st.write(f"{cause_index}. {cause}")
            else:
                st.write("No possible causes were returned.")

            if item["confirmed_causes"]:
                st.markdown("**Confirmed causes returned by the model**")
                for cause in item["confirmed_causes"]:
                    st.write(f"- {cause}")

            st.markdown("**Recommended actions**")
            if item["recommended_actions"]:
                for action in item["recommended_actions"]:
                    st.write(f"- {action}")
            else:
                st.write("No recommended actions were returned.")

            st.markdown("**Limitations**")
            st.write(item["limitations"])
            with st.expander("Source event metadata"):
                st.json({
                    "source": item["source"],
                    "diagnosis_status": item["diagnosis_status"],
                })

st.divider()
st.caption("Incident records are read from the existing ai-diagnosis Kafka topic. This page is read-only; it does not acknowledge, resolve, or modify incidents.")
