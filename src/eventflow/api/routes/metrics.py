from fastapi import APIRouter, Response

from eventflow.infrastructure.observability.metrics import metrics_body

router = APIRouter(tags=["observability"])


@router.get("/metrics")
async def prometheus_metrics() -> Response:
    body, content_type = metrics_body()
    return Response(content=body, media_type=content_type)
