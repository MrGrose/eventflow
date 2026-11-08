import argparse
import asyncio
import json
from pathlib import Path

from eventflow.config import get_settings
from eventflow.infrastructure.db.session import create_session_factory
from eventflow.lab.experiments import run_all


def _write_reports(transport: str, results: list[dict]) -> None:
    root = Path("docs/lab/results")
    root.mkdir(parents=True, exist_ok=True)
    for row in results:
        eid = row["experiment_id"]
        path = root / f"{transport}-{eid}.json"
        path.write_text(json.dumps(row, indent=2), encoding="utf-8")


async def main_async(transport: str) -> None:
    settings = get_settings()
    factory = create_session_factory(settings)
    results = await run_all(factory, transport)
    _write_reports(transport, results)
    print(json.dumps({"transport": transport, "experiments": len(results)}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=["rabbitmq", "kafka"], required=True)
    args = parser.parse_args()
    asyncio.run(main_async(args.transport))


if __name__ == "__main__":
    main()
