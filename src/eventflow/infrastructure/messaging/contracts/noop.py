from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest, PublishResult


class NoopPublisher:
    def __init__(self, *, fail: bool = False, error: str = "broker unavailable") -> None:
        self.fail = fail
        self.error = error
        self.published: list[PublishRequest] = []

    async def publish(self, message: PublishRequest) -> PublishResult:
        if self.fail:
            return PublishResult(accepted=False, error=self.error)
        self.published.append(message)
        return PublishResult(
            accepted=True, broker_meta={"transport": "noop", "event_id": message.event_id}
        )
