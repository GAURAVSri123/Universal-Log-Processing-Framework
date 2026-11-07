
from log_embedding_generator import LogEmbeddingGenerator
from elasticsearch_vector_store import ElasticsearchVectorStore


class LogRetriever:
    """
    Retrieves historically relevant logs using semantic vector search.
    """

    def __init__(self):
        print("[RETRIEVER] Initializing log retriever...")

        self.embedding_generator = LogEmbeddingGenerator()
        self.vector_store = ElasticsearchVectorStore()

        print("[RETRIEVER] Log retriever initialized.")

    def retrieve_logs(self, query, top_k=3):
        """
        Retrieve the most relevant historical logs for a query.

        Args:
            query (str): User's question or search text.
            top_k (int): Maximum number of logs to retrieve.

        Returns:
            list: Relevant historical log records.
        """

        if not isinstance(query, str) or not query.strip():
            raise ValueError("Search query cannot be empty.")

        if not isinstance(top_k, int) or isinstance(top_k, bool) or top_k < 1:
            raise ValueError("top_k must be a positive integer.")

        print(f"\n[RETRIEVER] Searching for: {query}")

        # Step 1: Convert the query into an embedding
        query_embedding = (
            self.embedding_generator.generate_embedding(query)
        )

        # Step 2: Search the Elasticsearch vector index
        results = self.vector_store.search_similar(
            query_embedding=query_embedding,
            top_k=top_k
        )

        # Step 3: Display retrieved records
        print(f"\n[RETRIEVER] Retrieved {len(results)} logs.")

        return results

    def close(self):
        """Close Elasticsearch connection."""
        self.vector_store.close()


if __name__ == "__main__":

    retriever = LogRetriever()

    try:
        search_query = "Why is the payment processing failing?"

        results = retriever.retrieve_logs(
            query=search_query,
            top_k=3
        )

        print("\n========== RETRIEVED LOGS ==========")

        for index, result in enumerate(results, start=1):

            print(f"\nResult {index}")
            print("Message:", result.get("message"))
            print("Service:", result.get("service"))
            print("Level:", result.get("level"))
            print("Classification:", result.get("classification"))
            print("Similarity score:", result.get("_score"))

    finally:
        retriever.close()

