import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "eventflow"
LAYER_RULES: dict[str, set[str]] = {
    "domain": {"infrastructure", "api", "workers"},
    "application": {"api", "workers"},
}


def _imports_in_package(package: str) -> list[str]:
    root = SRC / package
    imports: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
    return imports


def test_domain_does_not_import_outer_layers() -> None:
    forbidden = LAYER_RULES["domain"]
    for module in _imports_in_package("domain"):
        if module.startswith("eventflow."):
            inner = module.removeprefix("eventflow.").split(".")[0]
            assert inner not in forbidden, f"domain imports forbidden layer: {module}"
