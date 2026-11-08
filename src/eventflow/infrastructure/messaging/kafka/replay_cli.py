import argparse
import asyncio

from aiokafka import AIOKafkaConsumer

from eventflow.config import get_settings


async def _main(topic: str, limit: int) -> None:
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        n = 0
        async for msg in consumer:
            print(msg.topic, msg.partition, msg.offset, msg.key, len(msg.value))
            n += 1
            if n >= limit:
                break
    finally:
        await consumer.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=None)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    settings = get_settings()
    topic = args.topic or settings.kafka_topic_events
    asyncio.run(_main(topic, args.limit))


if __name__ == "__main__":
    main()
