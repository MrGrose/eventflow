from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

from eventflow.api.routes.metrics import router as metrics_router
from eventflow.api.routes.orders import router as orders_router
from eventflow.api.routes.ready import router as ready_router
from eventflow.config import get_settings
from eventflow.infrastructure.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("api_start", extra={"app_env": settings.app_env})
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="EventFlow", description="Orders API with transactional outbox.", lifespan=lifespan
    )
    app.include_router(orders_router)
    app.include_router(metrics_router)
    app.include_router(ready_router)

    @app.middleware("http")
    async def correlation_id_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID")
        if not correlation_id:
            correlation_id = request.headers.get("x-request-id")
        if correlation_id:
            request.state.correlation_id = correlation_id
        response = await call_next(request)
        if correlation_id:
            response.headers["X-Correlation-ID"] = correlation_id
        return response

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
