import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from eventflow.application.idempotency.consumer import run_idempotent
from eventflow.domain.reliability.errors import PermanentProcessingError, RetryableProcessingError
from eventflow.infrastructure.db.models.order import OrderModel
from eventflow.infrastructure.db.models.payment import PaymentModel
from eventflow.infrastructure.db.models.processed_event import ProcessedEventModel
from eventflow.infrastructure.idempotency.repository import ProcessedEventsRepository


@pytest.mark.asyncio
async def test_processed_events_dedupe(db_session) -> None:
    event_id = uuid4()
    repo = ProcessedEventsRepository(db_session)
    assert await repo.register_or_duplicate(consumer_name="billing", event_id=event_id)
    assert not await repo.register_or_duplicate(consumer_name="billing", event_id=event_id)
    await db_session.commit()


@pytest.mark.asyncio
async def test_run_idempotent_skips_duplicate(db_session) -> None:
    event_id = uuid4()
    calls = 0

    async def work() -> None:
        nonlocal calls
        calls += 1

    assert await run_idempotent(
        db_session, consumer_name="inventory", event_id=event_id, handler=work
    )
    assert calls == 1
    assert not await run_idempotent(
        db_session, consumer_name="inventory", event_id=event_id, handler=work
    )
    assert calls == 1


@pytest.mark.asyncio
async def test_concurrent_register_one_wins(db_session_factory) -> None:
    event_id = uuid4()

    async def once() -> bool:
        async with db_session_factory() as session:
            repo = ProcessedEventsRepository(session)
            ok = await repo.register_or_duplicate(consumer_name="billing", event_id=event_id)
            await session.commit()
            return ok

    results = await asyncio.gather(once(), once())
    assert results.count(True) == 1
    assert results.count(False) == 1


@pytest.mark.asyncio
async def test_run_idempotent_duplicate_does_not_commit_extraneous_changes(
    db_session_factory,
) -> None:
    event_id = uuid4()
    async with db_session_factory() as session:

        async def noop() -> None:
            return

        assert await run_idempotent(
            session, consumer_name="billing", event_id=event_id, handler=noop
        )
    uncommitted_order_id = uuid4()
    async with db_session_factory() as session:
        session.add(OrderModel(id=uncommitted_order_id, customer_id="dirty-caller", status="draft"))

        async def should_not_run() -> None:
            raise AssertionError("duplicate must not execute handler")

        is_processed = await run_idempotent(
            session, consumer_name="billing", event_id=event_id, handler=should_not_run
        )
        assert is_processed is False
        await session.rollback()
    async with db_session_factory() as session:
        leaked = await session.get(OrderModel, uncommitted_order_id)
        assert leaked is None


@pytest.mark.asyncio
async def test_run_idempotent_rollback_on_error_and_retryable_recovery(db_session_factory) -> None:
    event_id = uuid4()
    order_id = uuid4()
    async with db_session_factory() as session:

        async def failing_handler() -> None:
            session.add(OrderModel(id=order_id, customer_id="rollback-test", status="draft"))
            await session.flush()
            raise PermanentProcessingError("business rule failed after flush")

        with pytest.raises(PermanentProcessingError):
            await run_idempotent(
                session, consumer_name="billing", event_id=event_id, handler=failing_handler
            )
    async with db_session_factory() as session:
        assert await session.get(OrderModel, order_id) is None
        processed_count = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == event_id)
        )
        assert processed_count == 0
    attempts = 0
    retry_event_id = uuid4()
    recovered_order_id = uuid4()
    for expected_ok in (False, True):
        async with db_session_factory() as session:

            async def flaky_handler() -> None:
                nonlocal attempts
                attempts += 1
                session.add(
                    OrderModel(id=recovered_order_id, customer_id="retry-ok", status="confirmed")
                )
                await session.flush()
                if attempts == 1:
                    raise RetryableProcessingError("temporary timeout")

            if not expected_ok:
                with pytest.raises(RetryableProcessingError):
                    await run_idempotent(
                        session,
                        consumer_name="billing",
                        event_id=retry_event_id,
                        handler=flaky_handler,
                    )
            else:
                assert await run_idempotent(
                    session, consumer_name="billing", event_id=retry_event_id, handler=flaky_handler
                )
    async with db_session_factory() as session:
        assert await session.get(OrderModel, recovered_order_id) is not None
        processed_count = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == retry_event_id)
        )
        assert processed_count == 1


@pytest.mark.asyncio
async def test_external_modified_without_flush_preserved_across_duplicate_and_error(
    db_session_factory,
) -> None:
    order_id = uuid4()
    dup_event_id = uuid4()
    err_event_id = uuid4()
    async with db_session_factory() as seed_session:
        seed_session.add(OrderModel(id=order_id, customer_id="ext-no-flush", status="draft"))

        async def seed_noop() -> None:
            return

        await run_idempotent(
            seed_session, consumer_name="billing", event_id=dup_event_id, handler=seed_noop
        )
        await seed_session.commit()
    async with db_session_factory() as session:
        order = await session.get(OrderModel, order_id)
        assert order is not None
        order.status = "confirmed"

        async def noop() -> None:
            return

        res_dup = await run_idempotent(
            session, consumer_name="billing", event_id=dup_event_id, handler=noop
        )
        assert res_dup is False

        async def failing_work() -> None:
            session.add(
                PaymentModel(order_id=order_id, amount=Decimal("10.00"), status="completed")
            )
            await session.flush()
            raise PermanentProcessingError("handler failure")

        with pytest.raises(PermanentProcessingError):
            await run_idempotent(
                session, consumer_name="billing", event_id=err_event_id, handler=failing_work
            )
        async with db_session_factory() as other_session:
            other_view = await other_session.get(OrderModel, order_id)
            assert other_view is not None
            assert other_view.status == "draft"
        await session.commit()
    async with db_session_factory() as verify_session:
        saved = await verify_session.get(OrderModel, order_id)
        assert saved is not None
        assert saved.status == "confirmed"
        payments_count = await verify_session.scalar(
            select(func.count()).select_from(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        assert payments_count == 0


@pytest.mark.asyncio
async def test_external_modified_with_flush_not_rolled_back_or_committed_early(
    db_session_factory,
) -> None:
    order_id = uuid4()
    dup_event_id = uuid4()
    ok_event_id = uuid4()
    async with db_session_factory() as seed_session:
        seed_session.add(OrderModel(id=order_id, customer_id="ext-flushed", status="draft"))

        async def seed_noop() -> None:
            return

        await run_idempotent(
            seed_session, consumer_name="billing", event_id=dup_event_id, handler=seed_noop
        )
        await seed_session.commit()
    async with db_session_factory() as session:
        order = await session.get(OrderModel, order_id)
        assert order is not None
        order.status = "confirmed"
        await session.flush()

        async def noop() -> None:
            return

        assert (
            await run_idempotent(
                session, consumer_name="billing", event_id=dup_event_id, handler=noop
            )
            is False
        )

        async def record_payment() -> None:
            session.add(
                PaymentModel(order_id=order_id, amount=Decimal("25.00"), status="completed")
            )

        assert (
            await run_idempotent(
                session, consumer_name="billing", event_id=ok_event_id, handler=record_payment
            )
            is True
        )
        async with db_session_factory() as other_session:
            other_order = await other_session.get(OrderModel, order_id)
            assert other_order is not None
            assert other_order.status == "draft"
            other_payments = await other_session.scalar(
                select(func.count())
                .select_from(PaymentModel)
                .where(PaymentModel.order_id == order_id)
            )
            assert other_payments == 0
        await session.commit()
    async with db_session_factory() as verify_session:
        final_order = await verify_session.get(OrderModel, order_id)
        assert final_order is not None
        assert final_order.status == "confirmed"
        final_payments = await verify_session.scalar(
            select(func.count()).select_from(PaymentModel).where(PaymentModel.order_id == order_id)
        )
        assert final_payments == 1


@pytest.mark.asyncio
async def test_external_added_with_flush_preserved_across_duplicate_error_and_success(
    db_session_factory,
) -> None:
    ext_order_id = uuid4()
    dup_event_id = uuid4()
    err_event_id = uuid4()
    ok_event_id = uuid4()
    async with db_session_factory() as seed_session:

        async def seed_noop() -> None:
            return

        await run_idempotent(
            seed_session, consumer_name="billing", event_id=dup_event_id, handler=seed_noop
        )
    async with db_session_factory() as session:
        session.add(
            OrderModel(id=ext_order_id, customer_id="added-and-flushed", status="confirmed")
        )
        await session.flush()

        async def noop() -> None:
            return

        assert (
            await run_idempotent(
                session, consumer_name="billing", event_id=dup_event_id, handler=noop
            )
            is False
        )
        assert await session.get(OrderModel, ext_order_id) is not None

        async def failing_handler() -> None:
            session.add(
                PaymentModel(order_id=ext_order_id, amount=Decimal("99.00"), status="completed")
            )
            await session.flush()
            raise RetryableProcessingError("transient failure inside handler")

        with pytest.raises(RetryableProcessingError):
            await run_idempotent(
                session, consumer_name="billing", event_id=err_event_id, handler=failing_handler
            )
        assert await session.get(OrderModel, ext_order_id) is not None
        failed_payments = await session.scalar(
            select(func.count())
            .select_from(PaymentModel)
            .where(PaymentModel.order_id == ext_order_id)
        )
        assert failed_payments == 0
        failed_processed = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == err_event_id)
        )
        assert failed_processed == 0

        async def succeeding_handler() -> None:
            session.add(
                PaymentModel(order_id=ext_order_id, amount=Decimal("50.00"), status="completed")
            )

        assert (
            await run_idempotent(
                session, consumer_name="billing", event_id=ok_event_id, handler=succeeding_handler
            )
            is True
        )
        async with db_session_factory() as other_session:
            assert await other_session.get(OrderModel, ext_order_id) is None
            assert (
                await other_session.scalar(
                    select(func.count())
                    .select_from(ProcessedEventModel)
                    .where(ProcessedEventModel.event_id == ok_event_id)
                )
                == 0
            )
        await session.commit()
    async with db_session_factory() as verify_session:
        assert await verify_session.get(OrderModel, ext_order_id) is not None
        payments_count = await verify_session.scalar(
            select(func.count())
            .select_from(PaymentModel)
            .where(PaymentModel.order_id == ext_order_id)
        )
        assert payments_count == 1
        processed_count = await verify_session.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == ok_event_id)
        )
        assert processed_count == 1


@pytest.mark.asyncio
async def test_autobegin_select_then_in_transaction(db_session_factory) -> None:
    from sqlalchemy import text

    async with db_session_factory() as session:
        await session.execute(text("SELECT 1"))
        assert session.in_transaction() is True


@pytest.mark.asyncio
async def test_worker_clean_session_commit_persists(db_session_factory) -> None:
    event_id = uuid4()
    async with db_session_factory() as session:

        async def noop() -> None:
            return

        assert (
            await run_idempotent(session, consumer_name="billing", event_id=event_id, handler=noop)
            is True
        )
    async with db_session_factory() as check:
        count = await check.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == event_id)
        )
        assert count == 1


@pytest.mark.asyncio
async def test_autobegin_requires_caller_commit(db_session_factory) -> None:
    from sqlalchemy import text

    event_id = uuid4()
    async with db_session_factory() as session:
        await session.execute(text("SELECT 1"))
        assert session.in_transaction() is True

        async def noop() -> None:
            return

        assert (
            await run_idempotent(session, consumer_name="billing", event_id=event_id, handler=noop)
            is True
        )
    async with db_session_factory() as check:
        count = await check.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == event_id)
        )
        assert count == 0
    event_id2 = uuid4()
    async with db_session_factory() as session:
        await session.execute(text("SELECT 1"))

        async def noop() -> None:
            return

        assert await run_idempotent(
            session, consumer_name="billing", event_id=event_id2, handler=noop
        )
        await session.commit()
    async with db_session_factory() as check:
        count = await check.scalar(
            select(func.count())
            .select_from(ProcessedEventModel)
            .where(ProcessedEventModel.event_id == event_id2)
        )
        assert count == 1
