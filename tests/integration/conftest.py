import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from eventflow.api.app import create_app
from eventflow.config import get_settings
from eventflow.infrastructure.db.base import Base
from eventflow.infrastructure.db.session import create_session_factory

DEFAULT_PG = "postgresql+asyncpg://eventflow:eventflow@localhost:5435/eventflow"
_TRUNCATE_SQL = (
    "TRUNCATE processed_events, outbox_events, order_items, orders, payments, "
    "stock_items RESTART IDENTITY CASCADE"
)


def _database_url() -> str:
    url = os.getenv("DATABASE_URL", DEFAULT_PG)
    if url.endswith("@localhost:5432/eventflow"):
        url = DEFAULT_PG
    os.environ["DATABASE_URL"] = url
    return url


@pytest_asyncio.fixture
async def db_engine():
    url = _database_url()
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text(_TRUNCATE_SQL))
        yield engine
    except OSError as exc:
        pytest.skip(f"database unreachable: {exc}")
    except Exception as exc:
        pytest.skip(f"database unavailable: {exc}")
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db_session_factory(db_engine) -> async_sessionmaker[AsyncSession]:
    settings = get_settings()
    return create_session_factory(settings)


@pytest_asyncio.fixture
async def db_session(db_session_factory) -> AsyncSession:
    async with db_session_factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def api_client(db_engine, db_session_factory):
    app = create_app()
    app.state.session_factory = db_session_factory
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
