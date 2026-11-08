import logging

from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from eventflow.config import Settings, get_settings

logger = logging.getLogger(__name__)


async def ensure_topics(settings: Settings | None = None) -> None:
    cfg = settings or get_settings()
    admin = AIOKafkaAdminClient(bootstrap_servers=cfg.kafka_bootstrap_servers)
    await admin.start()
    try:
        existing = await admin.list_topics()
        desired = [
            NewTopic(
                name=cfg.kafka_topic_events,
                num_partitions=cfg.kafka_partitions_events,
                replication_factor=1,
            ),
            NewTopic(
                name=cfg.kafka_topic_retry,
                num_partitions=cfg.kafka_partitions_events,
                replication_factor=1,
            ),
            NewTopic(
                name=cfg.kafka_topic_dlt,
                num_partitions=cfg.kafka_partitions_dlt,
                replication_factor=1,
            ),
        ]
        to_create = [t for t in desired if t.name not in existing]
        if to_create:
            await admin.create_topics(to_create, validate_only=False)
            logger.info("kafka_topics_created %s", [t.name for t in to_create])
    finally:
        await admin.close()
