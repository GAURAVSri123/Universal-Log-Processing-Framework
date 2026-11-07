import streamlit as st
import pandas as pd
import plotly.express as px

from frontend.data_service import DashboardDataService


# ---------------- PAGE CONFIGURATION ----------------

st.set_page_config(
    page_title="Log Intelligence Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ---------------- CUSTOM STYLING ----------------

st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    div[data-testid="stMetric"] {
        background-color: #1c2333;
        border: 1px solid #30394b;
        padding: 18px;
        border-radius: 12px;
    }

    div[data-testid="stMetricLabel"] {
        color: #b9c3d5;
    }

    div[data-testid="stMetricValue"] {
        color: #ffffff;
    }

    .dashboard-subtitle {
        color: #a7b0c0;
        font-size: 15px;
    }
</style>
""", unsafe_allow_html=True)


# ---------------- SIDEBAR ----------------

with st.sidebar:
    st.title("🛡️ Log Intelligence")
    st.caption("Universal Log Processing Framework")

    st.divider()

    st.markdown("### Navigation")
    st.markdown("📊 **Overview Dashboard**")
    st.markdown("🔎 Log Explorer")
    st.markdown("🤖 AI Assistant")
    st.markdown("🧠 AI Investigations")
    st.markdown("🚨 Incidents")
    st.markdown("📥 Dead Letter Queue")

    st.divider()

    st.caption("Infrastructure")
    st.success("Elasticsearch configured")
    st.info("Data source: processed-logs")

    refresh = st.button(
        "🔄 Refresh Dashboard",
        use_container_width=True
    )


# ---------------- HEADER ----------------

st.title("📊 Overview Dashboard")

st.markdown(
    '<p class="dashboard-subtitle">'
    'Real-time visibility into processed logs, system activity, '
    'service health, and error patterns.'
    '</p>',
    unsafe_allow_html=True
)

st.divider()


# ---------------- LOAD DATA ----------------

@st.cache_resource
def get_data_service():
    return DashboardDataService()


try:
    service = get_data_service()
    summary = service.get_dashboard_summary()
    recent_logs = service.get_recent_logs()
    daily_trend = service.get_daily_trend()

except Exception as error:
    st.error(
        "Unable to load dashboard data from Elasticsearch."
    )
    st.exception(error)
    st.stop()


# ---------------- KPI METRICS ----------------

severity = {
    key.upper(): value
    for key, value in summary["severity"].items()
}

priority = {
    key.upper(): value
    for key, value in summary["priority"].items()
}

total_logs = summary["total_logs"]
error_logs = severity.get("ERROR", 0)
warning_logs = severity.get("WARNING", 0)
critical_logs = (
    priority.get("P1", 0)
    + severity.get("CRITICAL", 0)
)

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Processed Logs",
    f"{total_logs:,}"
)

col2.metric(
    "Error Logs",
    f"{error_logs:,}"
)

col3.metric(
    "Warning Logs",
    f"{warning_logs:,}"
)

col4.metric(
    "Critical / P1 Logs",
    f"{critical_logs:,}"
)

st.caption(
    f"Knowledge base records: "
    f"{summary['knowledge_base_logs']:,}"
)

st.divider()


# ---------------- CHARTS ----------------

left, right = st.columns(2)

with left:
    st.subheader("Log Severity Distribution")

    if summary["severity"]:

        severity_df = pd.DataFrame(
            list(summary["severity"].items()),
            columns=["Severity", "Count"]
        )

        fig = px.pie(
            severity_df,
            names="Severity",
            values="Count",
            hole=0.55,
            title="Distribution by severity"
        )

        fig.update_layout(
            template="plotly_dark",
            height=350
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    else:
        st.info("No severity data available.")


with right:
    st.subheader("Priority Distribution")

    if summary["priority"]:

        priority_df = pd.DataFrame(
            list(summary["priority"].items()),
            columns=["Priority", "Count"]
        )

        priority_df = priority_df.sort_values(
            "Priority"
        )

        fig = px.bar(
            priority_df,
            x="Priority",
            y="Count",
            text="Count",
            title="Logs grouped by priority"
        )

        fig.update_layout(
            template="plotly_dark",
            height=350,
            xaxis_title="Priority",
            yaxis_title="Number of logs"
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    else:
        st.info("No priority data available.")


st.divider()


# ---------------- DAILY TREND ----------------

st.subheader("📈 Daily Log Activity")

if daily_trend:

    trend_df = pd.DataFrame(daily_trend)

    trend_df["date"] = pd.to_datetime(
        trend_df["date"]
    )

    fig = px.line(
        trend_df,
        x="date",
        y="count",
        markers=True,
        title="Processed logs over time"
    )

    fig.update_layout(
        template="plotly_dark",
        height=350,
        xaxis_title="Date",
        yaxis_title="Log count"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

else:
    st.info("No timestamp trend data available.")


st.divider()


# ---------------- SERVICE MONITORING ----------------

st.subheader("🖥️ Service Monitoring")

if summary["services"]:

    service_df = pd.DataFrame(
        list(summary["services"].items()),
        columns=["Service", "Log Count"]
    )

    service_df = service_df.sort_values(
        "Log Count",
        ascending=False
    )

    st.dataframe(
        service_df,
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("No service data available.")


st.divider()


# ---------------- RECENT LOGS ----------------

st.subheader("📋 Recent Log Activity")

if recent_logs:

    logs_df = pd.DataFrame(recent_logs)

    display_columns = [
        field for field in [
            "timestamp",
            "service",
            "level",
            "priority",
            "classification",
            "message"
        ]
        if field in logs_df.columns
    ]

    st.dataframe(
        logs_df[display_columns],
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("No processed logs found.")


# ---------------- FOOTER ----------------

st.divider()

st.caption(
    "Universal Log Processing Framework with AI Analysis | "
    "Powered by Kafka, Elasticsearch, RAG and Groq"
)