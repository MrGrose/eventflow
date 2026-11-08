from fastapi.testclient import TestClient

from eventflow.api.app import create_app


def test_metrics_endpoint() -> None:
    client = TestClient(create_app())
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "eventflow_events_published_total" in response.text
