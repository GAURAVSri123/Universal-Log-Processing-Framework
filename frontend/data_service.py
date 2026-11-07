from elasticsearch import Elasticsearch
from .config import (
    ELASTICSEARCH_URL,
    LOG_INDEX,
    KNOWLEDGE_INDEX,
    DASHBOARD_LOG_LIMIT,
    RECENT_LOG_LIMIT,
)


class DashboardDataService:

    def __init__(self):
        self.client = Elasticsearch(ELASTICSEARCH_URL)

        if not self.client.ping():
            raise ConnectionError(
                "Unable to connect to Elasticsearch."
            )

    def get_index_count(self, index):
        if not self.client.indices.exists(index=index):
            return 0

        return self.client.count(index=index)["count"]

    def get_dashboard_summary(self):
        if not self.client.indices.exists(index=LOG_INDEX):
            raise RuntimeError(
                f"Elasticsearch index '{LOG_INDEX}' does not exist."
            )

        response = self.client.search(
            index=LOG_INDEX,
            size=0,
            aggs={
                "severity_counts": {
                    "terms": {"field": "level", "size": 20}
                },
                "priority_counts": {
                    "terms": {"field": "priority", "size": 10}
                },
                "service_counts": {
                    "terms": {"field": "service", "size": 20}
                },
                "classification_counts": {
                    "terms": {
                        "field": "classification",
                        "size": 20
                    }
                },
            }
        )

        aggs = response["aggregations"]

        return {
            "total_logs": self.get_index_count(LOG_INDEX),
            "knowledge_base_logs": self.get_index_count(
                KNOWLEDGE_INDEX
            ),
            "severity": {
                item["key"]: item["doc_count"]
                for item in aggs["severity_counts"]["buckets"]
            },
            "priority": {
                item["key"]: item["doc_count"]
                for item in aggs["priority_counts"]["buckets"]
            },
            "services": {
                item["key"]: item["doc_count"]
                for item in aggs["service_counts"]["buckets"]
            },
            "classifications": {
                item["key"]: item["doc_count"]
                for item in aggs["classification_counts"]["buckets"]
            },
        }

    def get_recent_logs(self, limit=RECENT_LOG_LIMIT):
        response = self.client.search(
            index=LOG_INDEX,
            size=limit,
            sort=[
                {"timestamp": {"order": "desc", "unmapped_type": "date"}}
            ],
            query={"match_all": {}}
        )

        logs = []

        for hit in response["hits"]["hits"]:
            log = hit["_source"].copy()
            log["_id"] = hit["_id"]
            logs.append(log)

        return logs

    def get_daily_trend(self):
        response = self.client.search(
            index=LOG_INDEX,
            size=0,
            aggs={
                "daily_logs": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "day",
                        "min_doc_count": 0
                    }
                }
            }
        )

        return [
            {
                "date": bucket["key_as_string"],
                "count": bucket["doc_count"]
            }
            for bucket in response["aggregations"][
                "daily_logs"
            ]["buckets"]
        ]

    def close(self):
        self.client.close()