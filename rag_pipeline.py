import json

from rag_context_builder import RAGContextBuilder
from llm_diagnosis import LLMDiagnosis


class RAGPipeline:
    """
    Complete RAG-based AI log analysis pipeline.

    The retriever and embedding model are initialized only when
    historical log retrieval is actually requested.
    """

    def __init__(self, top_k=3):

        print("[RAG] Initializing RAG pipeline...")

        self._retriever = None

        self.context_builder = RAGContextBuilder(
            max_logs=top_k
        )

        self.llm_diagnosis = LLMDiagnosis()
        self.top_k = top_k

        print("[RAG] Pipeline initialized successfully.")

    @property
    def retriever(self):
        """Create the retriever only when historical search is needed."""

        if self._retriever is None:
            print("[RAG] Initializing retriever on first use...")

            from log_retriever import LogRetriever

            self._retriever = LogRetriever()

            print("[RAG] Retriever initialized successfully.")

        return self._retriever

    def analyze_log(self, current_log):
        """Analyze a log using semantic retrieval and an LLM."""

        if not isinstance(current_log, dict):
            raise TypeError(
                "Current log must be a dictionary."
            )

        message = current_log.get("message")

        if not isinstance(message, str) or not message.strip():
            raise ValueError(
                "Current log must contain a message."
            )

        print("\n[RAG] Starting log analysis...")

        search_query = (
            f"Service: {current_log.get('service', 'Unknown')}\n"
            f"Level: {current_log.get('level', 'Unknown')}\n"
            f"Classification: "
            f"{current_log.get('classification', 'Unknown')}\n"
            f"Message: {message}"
        )

        # Accessing self.retriever initializes it only on this path.
        historical_logs = self.retriever.retrieve_logs(
            query=search_query,
            top_k=self.top_k
        )

        print(
            f"[RAG] Retrieved {len(historical_logs)} "
            "relevant historical logs."
        )

        context = self.context_builder.build_context(
            current_log=current_log,
            historical_logs=historical_logs
        )

        print("[RAG] Context generated successfully.")

        diagnosis = self.llm_diagnosis.diagnose(
            rag_context=context
        )

        print("[RAG] AI diagnosis completed successfully.")

        return {
            "current_log": current_log,
            "retrieved_logs": historical_logs,
            "rag_context": context,
            "diagnosis": diagnosis
        }

    def close(self):
        """Close resources that were actually initialized."""

        print("\n[RAG] Closing pipeline resources...")

        try:
            if self._retriever is not None:
                self._retriever.close()

        finally:
            self.llm_diagnosis.close()

        print("[RAG] Pipeline resources closed.")


if __name__ == "__main__":

    pipeline = RAGPipeline(top_k=3)

    current_log = {
        "service": "payment-service",
        "level": "ERROR",
        "classification": "APPLICATION_ERROR",
        "priority": "P2",
        "message": "Payment transaction could not be completed"
    }

    try:

        result = pipeline.analyze_log(current_log)

        print("\n========== RETRIEVED LOG SUMMARY ==========")

        for number, log in enumerate(
            result["retrieved_logs"], start=1
        ):

            print(f"\nResult {number}")
            print("Message:", log.get("message"))
            print("Service:", log.get("service"))
            print("Level:", log.get("level"))
            print("Relevance:", log.get("_score"))

        print("\n========== FINAL AI LOG DIAGNOSIS ==========\n")

        print(
            json.dumps(
                result["diagnosis"],
                indent=4,
                ensure_ascii=False
            )
        )

    finally:
        pipeline.close()