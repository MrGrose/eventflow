import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from eventflow.config import Settings, get_settings
from eventflow.infrastructure.messaging.rabbitmq.topology import declare_topology

logger = logging.getLogger(__name__)


class RabbitMQSession:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._connection: AbstractRobustConnection | None = None
        self._publisher_channel: AbstractChannel | None = None
        self._topology_lock = asyncio.Lock()

    @property
    def connection(self) -> AbstractRobustConnection | None:
        return self._connection

    async def connect(self) -> None:
        if self._connection and (not self._connection.is_closed):
            return
        self._connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        self._connection.reconnect_callbacks.add(self._on_reconnect)
        await self._ensure_publisher_channel()

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        logger.warning("rabbitmq_reconnecting")
        self._publisher_channel = None
        channel = await connection.channel(publisher_confirms=True)
        async with self._topology_lock:
            await declare_topology(channel, self._settings)
        await channel.close()

    async def _ensure_publisher_channel(self) -> AbstractChannel:
        if self._connection is None or self._connection.is_closed:
            await self.connect()
        if self._publisher_channel is None or self._publisher_channel.is_closed:
            assert self._connection is not None
            self._publisher_channel = await self._connection.channel(publisher_confirms=True)
            async with self._topology_lock:
                await declare_topology(self._publisher_channel, self._settings)
        return self._publisher_channel

    async def consumer_channel(self) -> AbstractChannel:
        await self.connect()
        assert self._connection is not None
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._settings.rabbitmq_prefetch)
        async with self._topology_lock:
            await declare_topology(channel, self._settings)
        return channel

    async def publisher_channel(self) -> AbstractChannel:
        return await self._ensure_publisher_channel()

    async def close(self) -> None:
        if self._publisher_channel and (not self._publisher_channel.is_closed):
            await self._publisher_channel.close()
        if self._connection and (not self._connection.is_closed):
            await self._connection.close()
        self._publisher_channel = None
        self._connection = None

    async def __aenter__(self) -> "RabbitMQSession":
        await self.connect()
        return self

    async def __aexit__(self, *_exc) -> None:
        await self.close()
