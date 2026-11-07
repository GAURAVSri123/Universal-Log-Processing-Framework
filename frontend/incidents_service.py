"""Read-only incident view built from the existing AI diagnosis topic."""

from frontend.investigations_service import InvestigationsService


class IncidentsService:
    """Expose diagnosis events as incident records without creating a new pipeline.

    Each record represents one AI-diagnosed log event. This module does not infer
    that an incident is open or resolved because the current event schema does not
    contain an incident-lifecycle status.
    """

    def __init__(self):
        self.investigations = InvestigationsService()

    def fetch_incidents(self, limit=500):
        events, errors = self.investigations.fetch_investigations(limit=limit)
        incidents = []

        for event in events:
            log_data = event.get("log") or {}
            diagnosis = event.get("diagnosis") or {}
            if not isinstance(log_data, dict) or not isinstance(diagnosis, dict):
                continue

            incidents.append({
                "incident_id": event.get("event_id") or "unknown-event",
                "timestamp": event.get("analyzed_at") or log_data.get("timestamp") or "",
                "service": log_data.get("service") or "Unknown service",
                "log_level": str(log_data.get("level") or "UNKNOWN").upper(),
                "severity": str(diagnosis.get("severity") or "UNKNOWN").upper(),
                "priority": log_data.get("priority") or "—",
                "classification": log_data.get("classification") or "—",
                "message": log_data.get("message") or "No original log message available",
                "summary": diagnosis.get("problem_summary") or "No diagnosis summary available",
                "possible_causes": diagnosis.get("possible_causes") or [],
                "confirmed_causes": diagnosis.get("confirmed_causes") or [],
                "recommended_actions": diagnosis.get("recommended_actions") or [],
                "confidence": diagnosis.get("confidence") or "UNKNOWN",
                "limitations": diagnosis.get("limitations") or "Not specified",
                "diagnosis_status": event.get("diagnosis_status") or "UNKNOWN",
                "source": event.get("source") or {},
            })

        return incidents, errors
