
from elasticsearch import Elasticsearch
from datetime import datetime, timezone
from uuid import uuid4


ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "log-knowledge-base"
EMBEDDING_DIMENSION = 384


class ElasticsearchVectorStore:

    def __init__(self):
        """Initialize Elasticsearch connection."""

        self.client = Elasticsearch(ELASTICSEARCH_URL)

        if not self.client.ping():
            raise ConnectionError(
                "Unable to connect to Elasticsearch."
            )

        print("[VECTOR STORE] Connected to Elasticsearch.")

    def create_index(self):
        """Create the vector knowledge-base index."""

        if self.client.indices.exists(index=INDEX_NAME):
            print(f"[VECTOR STORE] Index '{INDEX_NAME}' already exists.")
            return

        mapping = {
            "properties": {
                "service": {"type": "keyword"},
                "level": {"type": "keyword"},
                "message": {"type": "text"},
                "classification": {"type": "keyword"},
                "priority": {"type": "keyword"},
                "embedding_text": {"type": "text"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMENSION,
                    "index": True,
                    "similarity": "cosine"
                },
                "timestamp": {"type": "date"},
                "record_type": {"type": "keyword"}
            }
        }

        self.client.indices.create(
            index=INDEX_NAME,
            mappings=mapping
        )

        print(f"[VECTOR STORE] Created index '{INDEX_NAME}'.")

    
    def index_log(
        self,
        log,
        embedding_result,
        document_id=None,
        source_index=None,
        source_id=None
    ):
        """Store a log, embedding and original source reference."""

        embedding = embedding_result.get("embedding")

        if not isinstance(embedding, list):
            raise ValueError("Embedding must be a list.")

        if len(embedding) != EMBEDDING_DIMENSION:
            raise ValueError(
                f"Expected {EMBEDDING_DIMENSION} dimensions, "
                f"received {len(embedding)}."
            )

        if not log.get("message"):
            raise ValueError("Log message cannot be empty.")

        document = {
            "service": log.get("service", "unknown"),
            "level": log.get("level", "UNKNOWN"),
            "message": log["message"],
            "classification": log.get(
                "classification", "UNCLASSIFIED"
            ),
            "priority": log.get("priority", "UNKNOWN"),
            "embedding_text": embedding_result["embedding_text"],
            "embedding": embedding,
            "timestamp": log.get(
                "timestamp",
                datetime.now(timezone.utc).isoformat()
            ),
            "record_type": log.get(
                "record_type", "historical_log"
            ),
            "attributes": log.get("attributes", {})
        }

        if source_index:
            document["source_index"] = source_index

        if source_id:
            document["source_id"] = source_id

        if document_id is None:
            document_id = str(uuid4())

        response = self.client.index(
            index=INDEX_NAME,
            id=document_id,
            document=document,
            refresh="wait_for"
        )

        print(
            f"[VECTOR STORE] Indexed log: {response['_id']}"
        )

        return response["_id"]


    def search_similar(self, query_embedding, top_k=3):
        """Find semantically similar historical logs."""

        if len(query_embedding) != EMBEDDING_DIMENSION:
            raise ValueError(
                "Query embedding has incorrect dimensions."
            )

        if not 1 <= top_k <= 100:
            raise ValueError("top_k must be between 1 and 100.")

        response = self.client.search(
    index=INDEX_NAME,
    knn={
        "field": "embedding",
        "query_vector": query_embedding,
        "k": top_k,
        "num_candidates": max(100, top_k * 10),
        "filter": {
            "bool": {
                "should": [
                    {"term": {"level": "ERROR"}},
                    {"term": {"level": "CRITICAL"}},
                    {"term": {"level": "WARNING"}},
                    {"term": {"level": "WARN"}},
                    {"term": {"level": "FATAL"}}
                ],
                "minimum_should_match": 1
            }
        }
    },
    size=top_k
)
        results = []

        for hit in response["hits"]["hits"]:
            document = hit["_source"].copy()
            document["_id"] = hit["_id"]
            document["_score"] = hit["_score"]

            results.append(document)

        return results

    def delete_log(self, document_id):
        """Delete a document by its ID."""

        if self.client.exists(
            index=INDEX_NAME,
            id=document_id
        ):
            self.client.delete(
                index=INDEX_NAME,
                id=document_id,
                refresh="wait_for"
            )

            print(
                f"[VECTOR STORE] Deleted test log: {document_id}"
            )

    def count_logs(self):
        """Return the number of documents in the index."""

        response = self.client.count(index=INDEX_NAME)
        return response["count"]

    def close(self):
        """Close Elasticsearch connection."""

        self.client.close()
        print("[VECTOR STORE] Connection closed.")


if __name__ == "__main__":

    from log_embedding_generator import LogEmbeddingGenerator

    generator = LogEmbeddingGenerator()
    vector_store = ElasticsearchVectorStore()

    demo_ids = []

    try:
        # Step 1: Create the index
        vector_store.create_index()

        # Step 2: Create sample historical logs
        historical_logs = [
            {
                "service": "database-service",
                "level": "ERROR",
                "classification": "DATABASE_ERROR",
                "priority": "P2",
                "message": "Database connection failed",
                "record_type": "demo"
            },
            {
                "service": "database-service",
                "level": "ERROR",
                "classification": "DATABASE_ERROR",
                "priority": "P2",
                "message": "Unable to connect to the database",
                "record_type": "demo"
            },
            {
                "service": "payment-service",
                "level": "ERROR",
                "classification": "APPLICATION_ERROR",
                "priority": "P2",
                "message": "Payment gateway request timed out",
                "record_type": "demo"
            }
        ]

        # Step 3: Generate embeddings and index logs
        for number, log in enumerate(historical_logs):

            embedding_result = generator.generate_log_embedding(log)

            document_id = f"demo-log-{number}"

            indexed_id = vector_store.index_log(
                log,
                embedding_result,
                document_id=document_id
            )

            demo_ids.append(indexed_id)

        # Step 4: Search using a new error
        new_error = {
            "service": "database-service",
            "level": "ERROR",
            "classification": "DATABASE_ERROR",
            "priority": "P2",
            "message": "Unable to establish a database connection"
        }

        query_result = generator.generate_log_embedding(new_error)

        similar_logs = vector_store.search_similar(
            query_result["embedding"],
            top_k=3
        )

        print("\n========== VECTOR SEARCH RESULTS ==========")

        for result in similar_logs:
            print("\nService:", result["service"])
            print("Message:", result["message"])
            print("Classification:", result["classification"])
            print("Similarity score:", result["_score"])

        print("\nTotal indexed documents:", vector_store.count_logs())

    finally:
        # Remove temporary demo records so the knowledge base
        # remains clean for the real integration.
        for document_id in demo_ids:
            vector_store.delete_log(document_id)

        vector_store.close()
