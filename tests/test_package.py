import tomllib
from importlib import import_module
from pathlib import Path


def test_package_is_importable() -> None:
    import_module("h2hdb_ingest")


def test_distribution_command_targets_python_module_main() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]

    assert project["name"] == "h2hdb-ingest"
    assert project["scripts"]["h2hdb-ingest"] == "h2hdb_ingest.__main__:main"
    assert project["scripts"]["h2hdb-ingest-bootstrap"] == (
        "h2hdb_ingest.bootstrap:main"
    )
    assert callable(import_module("h2hdb_ingest.__main__").main)
    assert callable(import_module("h2hdb_ingest.bootstrap").main)
