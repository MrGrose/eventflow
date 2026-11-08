from fastapi.testclient import TestClient

from eventflow.api.app import create_app


def test_health_and_ready() -> None:
    client = TestClient(create_app())
    assert client.get("/health").json() == {"status": "ok"}
    ready = client.get("/ready").json()
    assert ready["status"] in {"ready", "degraded"}
    assert "checks" in ready
    assert "transport" in ready
