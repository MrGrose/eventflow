import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "docs/lab/results"
DOC = ROOT / "docs/architecture/messaging-comparison.md"
MARKER_START = "<!-- LAB_RESULTS_TABLE -->"
MARKER_END = "<!-- /LAB_RESULTS_TABLE -->"


def _load(transport: str, eid: str) -> dict | None:
    path = RESULTS / f"{transport}-{eid}.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _format_metrics(metrics: dict) -> str:
    return ", ".join((f"{k}={v}" for k, v in metrics.items()))


def build_table() -> str:
    lines = ["| E | RabbitMQ (факты) | Kafka (факты) |", "|---|------------------|---------------|"]
    for eid in [f"E{i}" for i in range(1, 10)]:
        r = _load("rabbitmq", eid)
        k = _load("kafka", eid)
        rtxt = _format_metrics(r["metrics"]) if r else "—"
        ktxt = _format_metrics(k["metrics"]) if k else "—"
        lines.append(f"| {eid} | {rtxt} | {ktxt} |")
    return "\n".join(lines)


def main() -> None:
    table = build_table()
    text = DOC.read_text(encoding="utf-8")
    if MARKER_START not in text:
        text = text.rstrip() + f"\n\n{MARKER_START}\n{table}\n{MARKER_END}\n"
    else:
        before, rest = text.split(MARKER_START, 1)
        _, after = rest.split(MARKER_END, 1)
        text = f"{before}{MARKER_START}\n{table}\n{MARKER_END}{after}"
    DOC.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
