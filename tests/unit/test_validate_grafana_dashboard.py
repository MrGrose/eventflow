import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "observability/grafana/dashboards/eventflow.json"
SCRIPT = ROOT / "scripts" / "validate_grafana_dashboard.py"


def _load_validator():
    spec = importlib.util.spec_from_file_location("validate_grafana_dashboard", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_dashboard_json_uses_only_allowed_metrics() -> None:
    mod = _load_validator()
    data = json.loads(DASH.read_text(encoding="utf-8"))
    assert mod.validate_dashboard_json(data) == []
    assert data.get("panels")
