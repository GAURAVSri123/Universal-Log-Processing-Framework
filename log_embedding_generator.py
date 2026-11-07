
from sentence_transformers import SentenceTransformer
import numpy as np


class LogEmbeddingGenerator:
    """
    Generates semantic embeddings for normalized log records.

    Model: all-MiniLM-L6-v2
    Embedding dimensions: 384
    """

    MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
    EMBEDDING_DIMENSION = 384

    def __init__(self):
        print("[EMBEDDING] Loading model...")

        self.model = SentenceTransformer(self.MODEL_NAME)

        print("[EMBEDDING] Model loaded successfully.")

    def build_embedding_text(self, log):
        """
        Construct meaningful text from a normalized log.
        """

        if not isinstance(log, dict):
            raise TypeError("Log must be a dictionary.")

        message = log.get("message")

        if not isinstance(message, str) or not message.strip():
            raise ValueError("Log must contain a non-empty message.")

        parts = []

        fields = [
            ("Service", "service"),
            ("Level", "level"),
            ("Classification", "classification"),
            ("Priority", "priority"),
            ("Message", "message")
        ]

        for label, field in fields:
            value = log.get(field)

            if value is not None and str(value).strip():
                parts.append(f"{label}: {value}")

        return "\n".join(parts)

    def generate_embedding(self, text):
        """
        Convert text into a normalized 384-dimensional vector.
        """

        if not isinstance(text, str) or not text.strip():
            raise ValueError("Input text cannot be empty.")

        vector = self.model.encode(
            text,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False
        )

        if len(vector) != self.EMBEDDING_DIMENSION:
            raise ValueError(
                f"Expected {self.EMBEDDING_DIMENSION} dimensions, "
                f"got {len(vector)}."
            )

        return vector.astype(float).tolist()

    def generate_log_embedding(self, log):
        """
        Generate an embedding for a complete log record.
        """

        embedding_text = self.build_embedding_text(log)
        embedding = self.generate_embedding(embedding_text)

        return {
            "embedding_text": embedding_text,
            "embedding": embedding
        }

    def calculate_similarity(self, text1, text2):
        """
        Calculate cosine similarity between two texts.

        Since vectors are normalized, their dot product
        gives cosine similarity.
        """

        vector1 = self.generate_embedding(text1)
        vector2 = self.generate_embedding(text2)

        similarity = np.dot(vector1, vector2)

        return float(similarity)


if __name__ == "__main__":

    generator = LogEmbeddingGenerator()

    # Sample normalized log
    sample_log = {
        "service": "payment-service",
        "level": "ERROR",
        "classification": "APPLICATION_ERROR",
        "priority": "P2",
        "message": "Payment processing failed"
    }

    result = generator.generate_log_embedding(sample_log)

    print("\n========== EMBEDDING TEST ==========")

    print("Embedding text:")
    print(result["embedding_text"])

    print("\nEmbedding dimensions:")
    print(len(result["embedding"]))

    print("\nFirst 10 vector values:")
    print(result["embedding"][:10])

    # Semantic similarity test
    text1 = "Database connection failed"
    text2 = "Unable to connect to the database"

    similarity = generator.calculate_similarity(text1, text2)

    print("\n========== SIMILARITY TEST ==========")
    print(f"Text 1: {text1}")
    print(f"Text 2: {text2}")
    print(f"Cosine similarity: {similarity:.4f}")
