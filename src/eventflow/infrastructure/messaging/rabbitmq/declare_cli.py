import asyncio

from eventflow.infrastructure.messaging.rabbitmq.session import RabbitMQSession


async def main() -> None:
    async with RabbitMQSession() as session:
        await session.publisher_channel()
        print("topology declared")


if __name__ == "__main__":
    asyncio.run(main())
