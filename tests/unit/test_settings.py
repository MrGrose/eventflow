from eventflow.config import MessagingTransport, get_settings


def test_settings_defaults() -> None:
    settings = get_settings()
    assert settings.messaging_transport == MessagingTransport.RABBITMQ
    assert "asyncpg" in settings.database_url


def test_settings_transport_kafka(monkeypatch) -> None:
    monkeypatch.setenv("MESSAGING_TRANSPORT", "kafka")
    settings = get_settings()
    assert settings.messaging_transport == MessagingTransport.KAFKA
