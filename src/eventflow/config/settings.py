from enum import StrEnum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MessagingTransport(StrEnum):
    RABBITMQ = "rabbitmq"
    KAFKA = "kafka"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    app_env: str = "development"
    log_level: str = "INFO"
    database_url: str = Field(
        default="postgresql+asyncpg://eventflow:eventflow@localhost:5435/eventflow"
    )
    messaging_transport: MessagingTransport = MessagingTransport.RABBITMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_exchange_events: str = "eventflow.events"
    rabbitmq_exchange_dlx: str = "eventflow.dlx"
    rabbitmq_prefetch: int = Field(default=10, ge=1)
    rabbitmq_publish_confirm_timeout: float = Field(default=30.0, gt=0)
    rabbitmq_retry_ttl_ms: int = Field(default=5000, ge=1000)
    kafka_bootstrap_servers: str = "localhost:29092"
    kafka_topic_events: str = "eventflow.events"
    kafka_topic_retry: str = "eventflow.events.retry"
    kafka_topic_dlt: str = "eventflow.events.dlt"
    kafka_partitions_events: int = Field(default=6, ge=1)
    kafka_partitions_dlt: int = Field(default=3, ge=1)
    kafka_producer_acks: str = "all"
    kafka_producer_idempotent: bool = False
    kafka_consumer_auto_offset_reset: str = "earliest"
    kafka_session_timeout_ms: int = 30000
    kafka_max_retries: int = Field(default=3, ge=1)
    kafka_retry_backoff_ms: int = Field(default=200, ge=0)


def get_settings() -> Settings:
    return Settings()
