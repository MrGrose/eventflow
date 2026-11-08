from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.contracts.publisher import MessagePublisher
from eventflow.infrastructure.messaging.kafka.publisher import KafkaPublisher
from eventflow.infrastructure.messaging.kafka.session import KafkaSession
from eventflow.infrastructure.messaging.rabbitmq.publisher import RabbitMQPublisher
from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession


def publisher_for_settings(
    settings: Settings | None = None,
    *,
    rabbit_session: RabbitMQSession | None = None,
    kafka_session: KafkaSession | None = None,
) -> MessagePublisher:
    cfg = settings or get_settings()
    if cfg.messaging_transport.value == "kafka":
        session = kafka_session or KafkaSession(cfg)
        return KafkaPublisher(session, cfg)
    session = rabbit_session or RabbitMQSession(cfg)
    return RabbitMQPublisher(session, cfg)
