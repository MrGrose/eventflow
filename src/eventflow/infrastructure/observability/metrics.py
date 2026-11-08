from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

EVENTS_PUBLISHED = Counter(
    "eventflow_events_published_total",
    "Outbox events published to broker",
    ["transport", "outcome"],
)
EVENTS_PROCESSED = Counter(
    "eventflow_events_processed_total",
    "Consumer-processed events",
    ["transport", "consumer", "outcome"],
)
EVENTS_FAILED = Counter(
    "eventflow_events_failed_total", "Failed publish or consume", ["transport", "stage"]
)
PROCESSING_DURATION = Histogram(
    "eventflow_processing_duration_seconds", "Handler duration", ["transport", "consumer"]
)
RETRY_TOTAL = Counter("eventflow_retries_total", "Retry attempts", ["transport", "stage"])
DLQ_TOTAL = Counter(
    "eventflow_dlq_messages_total", "Messages routed to DLQ/DLT", ["transport", "consumer"]
)


def metrics_body() -> tuple[bytes, str]:
    return (generate_latest(), CONTENT_TYPE_LATEST)
