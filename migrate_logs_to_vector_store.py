
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from log_embedding_generator import LogEmbeddingGenerator
from elasticsearch_vector_store import ElasticsearchVectorStore


SOURCE_URL = "http://localhost:9200"
SOURCE_INDEX = "processed-logs"


def migrate_logs():

    print("\n========== LOG MIGRATION STARTED ==========")

    source_client = Elasticsearch(SOURCE_URL)

    if not source_client.ping():
        raise ConnectionError(
            "Cannot connect to source Elasticsearch."
        )

    generator = LogEmbeddingGenerator()
    vector_store = ElasticsearchVectorStore()

    migrated = 0
    failed = 0

    try:
        # Step 1: Ensure destination index exists
        vector_store.create_index()

        # Step 2: Read original logs
        query = {
            "query": {
                "match_all": {}
            }
        }

        logs = scan(
            client=source_client,
            index=SOURCE_INDEX,
            query=query,
            size=100
        )

        # Step 3: Generate and store embeddings
        for hit in logs:

            source_id = hit["_id"]
            log = hit["_source"].copy()

            try:
                if not log.get("message"):
                    print(
                        f"[SKIPPED] {source_id}: Missing message"
                    )
                    continue

                # Mark these as historical records,
                # not verified diagnoses.
                log["record_type"] = "historical_log"

                embedding_result = (
                    generator.generate_log_embedding(log)
                )

                # Deterministic ID prevents duplicates
                # when the migration is run again.
                destination_id = (
                    f"{SOURCE_INDEX}:{source_id}"
                )

                vector_store.index_log(
                    log=log,
                    embedding_result=embedding_result,
                    document_id=destination_id,
                    source_index=SOURCE_INDEX,
                    source_id=source_id
                )

                migrated += 1

            except Exception as error:
                failed += 1

                print(
                    f"[ERROR] Failed to migrate {source_id}: "
                    f"{error}"
                )

        # Step 4: Display migration summary
        print("\n========== MIGRATION SUMMARY ==========")

        print("Successfully migrated:", migrated)
        print("Failed records:", failed)
        print(
            "Total records in vector store:",
            vector_store.count_logs()
        )

    finally:
        source_client.close()
        vector_store.close()

    print("\n========== MIGRATION FINISHED ==========")


if __name__ == "__main__":
    migrate_logs()
