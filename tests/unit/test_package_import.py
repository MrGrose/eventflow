import eventflow
from eventflow.config import get_settings


def test_package_version() -> None:
    assert eventflow.__version__ == "0.1.0"


def test_settings_loads() -> None:
    assert get_settings().app_env
