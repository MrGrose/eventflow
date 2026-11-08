from eventflow.config import Settings, get_settings

CONSUMER_NAMES = ("billing", "inventory", "notifications", "analytics")


def consumer_group(consumer_name: str, settings: Settings | None = None) -> str:
    _ = settings or get_settings()
    return f"eventflow-{consumer_name}"


def topics(settings: Settings | None = None) -> tuple[str, str, str]:
    cfg = settings or get_settings()
    return (cfg.kafka_topic_events, cfg.kafka_topic_retry, cfg.kafka_topic_dlt)
