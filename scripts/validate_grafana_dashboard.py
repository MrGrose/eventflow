import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASH = ROOT / "observability/grafana/dashboards/eventflow.json"
PROVISION = ROOT / "observability/grafana/provisioning/dashboards/dashboards.yml"
ALLOWED = {
    "eventflow_events_published_total",
    "eventflow_events_processed_total",
    "eventflow_events_failed_total",
    "eventflow_processing_duration_seconds_bucket",
    "eventflow_retries_total",
    "eventflow_dlq_messages_total",
}


def validate_dashboard_json(data: dict) -> list[str]:
    exprs: list[str] = []
    for panel in data.get("panels", []):
        for target in panel.get("targets", []):
            if "expr" in target:
                exprs.append(target["expr"])
    return [expr for expr in exprs if not any(name in expr for name in ALLOWED)]


def grafana_settings() -> tuple[str, str, str | None]:
    url = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
    user = os.environ.get("GRAFANA_USER", "admin")
    password = os.environ.get("GRAFANA_PASSWORD")
    return url, user, password


def main() -> int:
    if not PROVISION.is_file():
        print("missing dashboards.yml provisioning", file=sys.stderr)
        return 1
    data = json.loads(DASH.read_text(encoding="utf-8"))
    unknown = validate_dashboard_json(data)
    for expr in unknown:
        print("unknown metric in expr:", expr, file=sys.stderr)
        return 1
    print("dashboard JSON ok, panels:", len(data.get("panels", [])))
    print("provisioning file ok:", PROVISION.name)
    base_url, user, password = grafana_settings()
    try:
        req = urllib.request.Request(f"{base_url}/api/health")
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                print("grafana health: ok")
    except (urllib.error.URLError, TimeoutError):
        print("grafana not running (file provisioning only)")
        return 0
    if not password:
        print("grafana API search skipped: GRAFANA_PASSWORD not set (см. .env.example)")
        return 0
    auth = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    auth.add_password(None, base_url, user, password)
    handler = urllib.request.HTTPBasicAuthHandler(auth)
    opener = urllib.request.build_opener(handler)
    try:
        search = opener.open(f"{base_url}/api/search?query=EventFlow", timeout=5)
        body = json.loads(search.read().decode())
        titles = [x.get("title") for x in body]
        if "EventFlow Overview" in titles:
            print("grafana dashboard provisioned: EventFlow Overview")
        else:
            print("grafana search (file may still load):", titles)
    except urllib.error.HTTPError as exc:
        print(
            f"grafana API search skipped: HTTP {exc.code} ",
            "(dashboard из file provisioning)",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
