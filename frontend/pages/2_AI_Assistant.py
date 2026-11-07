
import streamlit as st

from frontend.ai_service import AIAssistantService


st.set_page_config(
    page_title="AI Assistant",
    page_icon="🤖",
    layout="wide"
)


# ---------------- AI SERVICE ----------------

@st.cache_resource
def get_ai_service():
    return AIAssistantService()


# ---------------- SESSION STATE ----------------

if "ai_messages" not in st.session_state:
    st.session_state.ai_messages = []

if "ai_last_evidence" not in st.session_state:
    st.session_state.ai_last_evidence = []


# ---------------- HEADER ----------------

st.title("🤖 AI Log Assistant")

st.caption(
    "Ask questions about application failures and investigate "
    "them using historical log evidence and RAG."
)

st.divider()


# ---------------- SIDEBAR ----------------

with st.sidebar:
    st.subheader("AI Assistant")

    st.info(
        "The assistant retrieves similar historical logs "
        "before generating an answer."
    )

    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.ai_messages = []
        st.session_state.ai_last_evidence = []
        st.rerun()

    st.divider()

    st.caption("AI architecture")
    st.write("• Sentence Transformer embeddings")
    st.write("• Elasticsearch vector search")
    st.write("• RAG context retrieval")
    st.write("• Groq LLM")


# ---------------- CHAT HISTORY ----------------

for index, message in enumerate(st.session_state.ai_messages):

    with st.chat_message(message["role"]):
        st.markdown(message["content"])

        if (
            message["role"] == "assistant"
            and message.get("evidence")
        ):
            with st.expander("View retrieved historical logs"):
                for number, log in enumerate(
                    message["evidence"], start=1
                ):
                    st.markdown(f"**Historical log {number}**")
                    st.write(
                        "Service:",
                        log.get("service", "Unknown")
                    )
                    st.write(
                        "Severity:",
                        log.get("level", "Unknown")
                    )
                    st.write(
                        "Classification:",
                        log.get("classification", "Unknown")
                    )
                    st.write(
                        "Message:",
                        log.get("message", "Unknown")
                    )

                    if log.get("_score") is not None:
                        st.write(
                            "Similarity score:",
                            round(float(log["_score"]), 4)
                        )

                    st.divider()


# ---------------- CHAT INPUT ----------------

question = st.chat_input(
    "Ask about an error, service failure, or log pattern..."
)


if question:

    # Display and save the user's message.
    st.session_state.ai_messages.append({
        "role": "user",
        "content": question
    })

    with st.chat_message("user"):
        st.markdown(question)

    try:

        ai_service = get_ai_service()

        # Pass previous conversation turns to the AI service.
        history = st.session_state.ai_messages[:-1]

        with st.chat_message("assistant"):

            with st.spinner(
                "Retrieving historical logs and analysing evidence..."
            ):

                result = ai_service.ask(
                    question=question,
                    conversation_history=history
                )

            answer = result["answer"]
            evidence = result["retrieved_logs"]

            st.markdown(answer)

            with st.expander(
                f"View retrieved historical logs ({len(evidence)})"
            ):

                if not evidence:
                    st.info(
                        "No historical logs were retrieved."
                    )

                for number, log in enumerate(evidence, start=1):

                    st.markdown(f"**Historical log {number}**")

                    st.write(
                        "Service:",
                        log.get("service", "Unknown")
                    )

                    st.write(
                        "Severity:",
                        log.get("level", "Unknown")
                    )

                    st.write(
                        "Classification:",
                        log.get("classification", "Unknown")
                    )

                    st.write(
                        "Message:",
                        log.get("message", "Unknown")
                    )

                    if log.get("_score") is not None:
                        st.write(
                            "Similarity score:",
                            round(float(log["_score"]), 4)
                        )

                    st.divider()

        # Save assistant response and evidence.
        st.session_state.ai_messages.append({
            "role": "assistant",
            "content": answer,
            "evidence": evidence
        })

        st.session_state.ai_last_evidence = evidence

    except Exception as error:

        error_message = (
            "I couldn't complete the analysis. "
            "Please check the backend services and try again."
        )

        with st.chat_message("assistant"):
            st.error(error_message)
            st.exception(error)

        st.session_state.ai_messages.append({
            "role": "assistant",
            "content": error_message,
            "evidence": []
        })


# ---------------- FOOTER ----------------

st.divider()

st.caption(
    "AI Assistant | Existing RAG Pipeline + Elasticsearch "
    "Vector Search + Groq"
)
