from __future__ import annotations

import tomllib
from importlib import import_module
from pathlib import Path


def test_package_exports_only_vnext_consumer_boundaries() -> None:
    package = import_module("h2hdb_ingest")

    assert callable(package.VNextFilesystemSourceAdapter)
    assert callable(package.ManagedFilesystemLibraryAdapter)
    assert callable(package.ResidentIngestor)
    assert callable(package.VNextIngestService)
    assert issubclass(package.LibraryStorageIdentityMismatchError, RuntimeError)
    assert not hasattr(package, "H2HDB")
    assert not hasattr(package, "StagedIngestService")
    assert not hasattr(package, "LegacyIngestService")


def test_distribution_commands_target_vnext_entry_points() -> None:
    project = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]

    assert project["name"] == "h2hdb-ingest"
    assert project["version"] == "0.14.1"
    assert project["scripts"] == {
        "h2hdb-ingest": "h2hdb_ingest.__main__:main",
        "h2hdb-ingest-bootstrap": "h2hdb_ingest.bootstrap:main",
    }
    assert callable(import_module("h2hdb_ingest.__main__").main)
    assert callable(import_module("h2hdb_ingest.bootstrap").main)
