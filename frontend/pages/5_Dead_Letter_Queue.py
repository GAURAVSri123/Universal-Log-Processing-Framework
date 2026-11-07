import json
import streamlit as st
import pandas as pd

from frontend.dlq_service import fetch_dead_letter_records


st.set_page_config(page_title="Dead Letter Queue", page_icon="📬", layout="wide")


@st.cache_data(ttl=15, show_spinner=False)
def load_dlq_records():
    return fetch_dead_letter_records()


def pretty(value):
    try:
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


st.title("📬 Dead Letter Queue")
st.caption(
    "Inspect messages that could not be processed by the existing log-processing "
    "and AI-diagnosis services."
)

top_left, top_right = st.columns([5, 1])
with top_right:
    if st.button("↻ Refresh", use_container_width=True):
        load_dlq_records.clear()
        st.rerun()

try:
    with st.spinner("Reading retained dead-letter messages from Kafka..."):
        records = load_dlq_records()
except Exception as exc:
    st.error(f"Could not read the Kafka dead-letter topics: {exc}")
    st.info("Check that Kafka is running and that both DLQ topics are available.")
    st.stop()

if not records:
    st.info(
        "No retained dead-letter messages were found in "
        "`dead-letter-logs` or `ai-diagnosis-dlq`."
    )
    st.caption("This does not mean there have never been failures; Kafka retention may remove older messages.")
    st.stop()

df = pd.DataFrame(records)
total = len(df)
processing_count = int((df["dlq_topic"] == "dead-letter-logs").sum())
ai_count = int((df["dlq_topic"] == "ai-diagnosis-dlq").sum())
services = df["processor"].nunique()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total failed messages", total)
m2.metric("Log-processing DLQ", processing_count)
m3.metric("AI-diagnosis DLQ", ai_count)
m4.metric("Processors affected", services)

st.divider()
st.subheader("Dead-letter message records")

with st.sidebar:
    st.header("DLQ filters")
    topic_options = sorted(df["dlq_topic"].dropna().unique().tolist())
    selected_topics = st.multiselect("DLQ topic", topic_options, default=topic_options)
    processor_options = sorted(df["processor"].dropna().unique().tolist())
    selected_processors = st.multiselect("Processor", processor_options, default=processor_options)
    search_text = st.text_input("Search failure reason or payload", placeholder="e.g. invalid timestamp")

filtered = df[
    df["dlq_topic"].isin(selected_topics)
    & df["processor"].isin(selected_processors)
].copy()

if search_text.strip():
    needle = search_text.strip().lower()
    filtered = filtered[
        filtered.apply(
            lambda row: needle in (
                str(row["error"]) + " " + pretty(row["original"]) + " " + pretty(row["payload"])
            ).lower(),
            axis=1,
        )
    ]

st.caption(
    f"Showing {len(filtered)} of {total} retained messages. "
    "A DLQ entry indicates a processing failure; it is not automatically retried or resolved here."
)

if filtered.empty:
    st.warning("No messages match the selected filters.")
else:
    display_df = filtered[
        ["failed_at", "dlq_topic", "processor", "error", "source_topic",
         "source_partition", "source_offset"]
    ].rename(
        columns={
            "failed_at": "Failed at",
            "dlq_topic": "DLQ topic",
            "processor": "Processor",
            "error": "Failure reason",
            "source_topic": "Source topic",
            "source_partition": "Source partition",
            "source_offset": "Source offset",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.subheader("Message details")
    for index, row in filtered.reset_index(drop=True).iterrows():
        title = f"{row['dlq_topic']} · {row['processor']} · {row['failed_at']}"
        with st.expander(title):
            st.markdown(f"**Failure reason:** {row['error']}")
            st.markdown(
                f"**Original source:** `{row['source_topic']}` · "
                f"partition `{row['source_partition']}` · offset `{row['source_offset']}`"
            )
            st.markdown(
                f"**DLQ location:** `{row['dlq_topic']}` · "
                f"partition `{row['kafka_partition']}` · offset `{row['kafka_offset']}`"
            )
            st.markdown("**Original message / record**")
            st.code(pretty(row["original"]), language="json")
            with st.expander("View complete DLQ payload"):
                st.code(pretty(row["payload"]), language="json")

st.divider()
st.caption(
    "Read-only dashboard: it does not delete, replay, retry, or modify Kafka messages. "
    "Messages shown depend on Kafka topic retention."
)
