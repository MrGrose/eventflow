from collections import Counter

_counters: Counter[str] = Counter()


def inc_kafka_consumer(consumer: str, outcome: str) -> None:
    _counters[f"kafka_consumer_{consumer}_{outcome}"] += 1


def reset() -> None:
    _counters.clear()


def snapshot() -> dict[str, int]:
    return dict(_counters)
