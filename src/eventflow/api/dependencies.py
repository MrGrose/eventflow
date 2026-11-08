from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventflow.config import get_settings
from eventflow.infrastructure.db.session import create_session_factory


async def get_db_session(request: Request) -> AsyncIterator[AsyncSession]:
    factory: async_sessionmaker[AsyncSession] | None = getattr(
        request.app.state, "session_factory", None
    )
    if factory is None:
        settings = get_settings()
        factory = create_session_factory(settings)
        request.app.state.session_factory = factory
    async with factory() as session:
        yield session
