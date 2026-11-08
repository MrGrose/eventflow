from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from eventflow.config import Settings


def create_engine(database_url: str):
    return create_async_engine(database_url, pool_pre_ping=True)


def create_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(settings.database_url)
    return async_sessionmaker(engine, expire_on_commit=False)
