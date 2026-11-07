
from elasticsearch import Elasticsearch
import json

ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "processed-logs"


class ElasticsearchLogSearch:

    def __init__(self):
        """Initialize Elasticsearch connection."""

        self.client = Elasticsearch(ELASTICSEARCH_URL)

        if not self.client.ping():
            raise ConnectionError(
                "Unable to connect to Elasticsearch."
            )

        print("[ELASTICSEARCH] Search module connected.")

    def search_all(self, size=10):
        """Retrieve all recent logs."""

        response = self.client.search(
            index=INDEX_NAME,
            query={"match_all": {}},
            size=size,
            sort=[{"timestamp": {"order": "desc"}}]
        )

        return self._extract_logs(response)

    def search_by_level(self, level, size=10):
        """Search logs by severity."""

        response = self.client.search(
            index=INDEX_NAME,
            query={"term": {"level": level.upper()}},
            size=size
        )

        return self._extract_logs(response)

    def search_by_service(self, service, size=10):
        """Search logs by service name."""

        response = self.client.search(
            index=INDEX_NAME,
            query={"term": {"service": service}},
            size=size
        )

        return self._extract_logs(response)

    def search_by_priority(self, priority, size=10):
        """Search logs by priority."""

        response = self.client.search(
            index=INDEX_NAME,
            query={"term": {"priority": priority.upper()}},
            size=size
        )

        return self._extract_logs(response)

    def search_by_message(self, keyword, size=10):
        """Search logs by message keyword."""

        response = self.client.search(
            index=INDEX_NAME,
            query={"match": {"message": keyword}},
            size=size
        )

        return self._extract_logs(response)

    def _extract_logs(self, response):
        """Extract log documents from Elasticsearch response."""

        logs = []

        for hit in response["hits"]["hits"]:
            log = hit["_source"].copy()
            log["_id"] = hit["_id"]
            logs.append(log)

        return logs

    def close(self):
        """Close Elasticsearch connection."""

        self.client.close()
        print("[ELASTICSEARCH] Connection closed.")


if __name__ == "__main__":

    searcher = ElasticsearchLogSearch()

    try:

        print("\n========== ALL LOGS ==========")
        print(json.dumps(searcher.search_all(), indent=4))

        print("\n========== ERROR LOGS ==========")
        print(json.dumps(
            searcher.search_by_level("ERROR"),
            indent=4
        ))

        print("\n========== PAYMENT SERVICE ==========")
        print(json.dumps(
            searcher.search_by_service("payment-service"),
            indent=4
        ))

        print("\n========== PRIORITY P2 ==========")
        print(json.dumps(
            searcher.search_by_priority("P2"),
            indent=4
        ))

        print("\n========== MESSAGE SEARCH ==========")
        print(json.dumps(
            searcher.search_by_message("Payment processing failed"),
            indent=4
        ))

    finally:
        searcher.close()
