import asyncio
import warnings
from uuid import uuid4

import pytest

from eventflow.infrastructure.messaging.contracts.publisher import PublishRequest
from eventflow.lab.broker import lab_broker

pytestmark = [pytest.mark.lab, pytest.mark.kafka]


def _unclosed_kafka(warnings_list: list[warnings.WarningMessage]) -> list[str]:
    return [str(w.message) for w in warnings_list if "Unclosed AIOKafkaProducer" in str(w.message)]


@pytest.mark.asyncio
async def test_lab_broker_kafka_closes_clean() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with lab_broker("kafka") as br:
            await br.publisher.publish(
                PublishRequest(
                    event_id=str(uuid4()),
                    event_type="order.created",
                    aggregate_type="order",
                    aggregate_id=str(uuid4()),
                    payload={"lab": "lifecycle"},
                    target_transport="kafka",
                )
            )
        await asyncio.sleep(0.2)
    assert _unclosed_kafka(caught) == []


@pytest.mark.asyncio
async def test_lab_broker_kafka_closes_on_exception() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            async with lab_broker("kafka"):
                raise RuntimeError("simulated harness failure")
        except RuntimeError:
            pass
        await asyncio.sleep(0.2)
    assert _unclosed_kafka(caught) == []


@pytest.mark.asyncio
async def test_e5_parallel_workers_own_producers() -> None:

    async def worker() -> None:
        async with lab_broker("kafka") as br:
            await br.publisher.publish(
                PublishRequest(
                    event_id=str(uuid4()),
                    event_type="order.created",
                    aggregate_type="order",
                    aggregate_id=str(uuid4()),
                    payload={},
                    target_transport="kafka",
                )
            )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await asyncio.gather(worker(), worker(), worker())
        await asyncio.sleep(0.2)
    assert _unclosed_kafka(caught) == []
