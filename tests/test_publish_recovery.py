from __future__ import annotations

import hashlib
import json
import runpy
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable
from email.message import Message
from pathlib import Path
from typing import Any, cast

import pytest
from packaging.version import Version
from pytest import MonkeyPatch

AuthorizeRecovery = Callable[[str, str, str], Version]


def _authorize_recovery() -> AuthorizeRecovery:
    namespace = runpy.run_path(
        str(Path(__file__).parents[1] / "scripts" / "check-publish-recovery.py")
    )
    return cast(AuthorizeRecovery, namespace["_authorize_recovery"])


def _dependency_manifest(
    document: dict[str, Any],
    package: dict[str, Any],
) -> bytes:
    project = document.get("project", {})
    return json.dumps(
        {
            "build": document.get("build-system", {}).get("requires", []),
            "dependency-groups": document.get("dependency-groups", {}),
            "optional": project.get("optional-dependencies", {}),
            "runtime": project.get("dependencies", []),
            "node-dev": package.get("devDependencies", {}),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _fixture_repository(tmp_path: Path) -> tuple[Path, str]:
    repository = tmp_path / "repository"
    release = repository / ".release"
    release.mkdir(parents=True)
    project_text = """\
[build-system]
requires = ["hatchling>=1.32.0,<2.0.0"]

[project]
name = "h2hdb-ingest"
version = "0.9.1"
dependencies = ["h2hdb>=0.26.0,<0.27"]

[project.optional-dependencies]
dev = ["pytest>=9.1.1"]
"""
    package: dict[str, Any] = {"devDependencies": {"markdownlint-cli2": ">=0.23.2"}}
    (repository / "pyproject.toml").write_text(project_text, encoding="utf-8")
    (repository / "package.json").write_text(
        json.dumps(package),
        encoding="utf-8",
    )
    document = {
        "build-system": {"requires": ["hatchling>=1.32.0,<2.0.0"]},
        "project": {
            "name": "h2hdb-ingest",
            "version": "0.9.1",
            "dependencies": ["h2hdb>=0.26.0,<0.27"],
            "optional-dependencies": {"dev": ["pytest>=9.1.1"]},
        },
    }
    receipt = {
        "schema": "h2h.dependency-audit.v1",
        "project_version": "0.9.1",
        "manifest_sha256": hashlib.sha256(
            _dependency_manifest(document, package)
        ).hexdigest(),
        "review": {"status": "reviewed", "note": "fixture review"},
    }
    (release / "dependency-audit.json").write_text(
        json.dumps(receipt),
        encoding="utf-8",
    )
    subprocess.run(("git", "init", "-b", "main"), cwd=repository, check=True)
    subprocess.run(
        ("git", "config", "user.name", "Recovery Test"),
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ("git", "config", "user.email", "recovery@example.invalid"),
        cwd=repository,
        check=True,
    )
    subprocess.run(("git", "add", "."), cwd=repository, check=True)
    subprocess.run(
        ("git", "commit", "-m", "chore: seed recovery fixture"),
        cwd=repository,
        check=True,
    )
    revision = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return repository, revision


def _report_missing(*_args: object, **_kwargs: object) -> None:
    raise urllib.error.HTTPError(
        "https://pypi.org/pypi/h2hdb-ingest/0.9.1/json",
        404,
        "Not Found",
        Message(),
        None,
    )


def test_recovery_authorizes_one_exact_unpublished_revision(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, revision = _fixture_repository(tmp_path)
    monkeypatch.chdir(repository)
    monkeypatch.setattr(urllib.request, "urlopen", _report_missing)

    current = _authorize_recovery()("0.9.1", revision, "https://pypi.org")

    assert current == Version("0.9.1")


def test_recovery_rejects_a_different_revision(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, _revision = _fixture_repository(tmp_path)
    monkeypatch.chdir(repository)
    monkeypatch.setattr(urllib.request, "urlopen", _report_missing)

    with pytest.raises(ValueError, match="recovery revision mismatch"):
        _authorize_recovery()("0.9.1", "f" * 40, "https://pypi.org")


def test_recovery_rejects_a_different_version(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, revision = _fixture_repository(tmp_path)
    monkeypatch.chdir(repository)
    monkeypatch.setattr(urllib.request, "urlopen", _report_missing)

    with pytest.raises(ValueError, match="recovery is restricted to"):
        _authorize_recovery()("0.9.0", revision, "https://pypi.org")


def test_recovery_rejects_stale_dependency_audit(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, revision = _fixture_repository(tmp_path)
    receipt_path = repository / ".release" / "dependency-audit.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["manifest_sha256"] = "0" * 64
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    subprocess.run(
        ("git", "add", ".release/dependency-audit.json"),
        cwd=repository,
        check=True,
    )
    subprocess.run(
        ("git", "commit", "-m", "test: stale dependency audit"),
        cwd=repository,
        check=True,
    )
    revision = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    monkeypatch.chdir(repository)
    monkeypatch.setattr(urllib.request, "urlopen", _report_missing)

    with pytest.raises(ValueError, match="does not match dependencies"):
        _authorize_recovery()("0.9.1", revision, "https://pypi.org")


def test_recovery_rejects_a_dirty_checkout(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, revision = _fixture_repository(tmp_path)
    (repository / "untracked.txt").write_text("unexpected\n", encoding="utf-8")
    monkeypatch.chdir(repository)
    monkeypatch.setattr(urllib.request, "urlopen", _report_missing)

    with pytest.raises(ValueError, match="not an exact clean revision"):
        _authorize_recovery()("0.9.1", revision, "https://pypi.org")


def test_recovery_rejects_an_already_published_version(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    repository, revision = _fixture_repository(tmp_path)
    monkeypatch.chdir(repository)

    class PublishedResponse:
        def __enter__(self) -> PublishedResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    def report_published(*_args: object, **_kwargs: object) -> PublishedResponse:
        return PublishedResponse()

    monkeypatch.setattr(urllib.request, "urlopen", report_published)

    with pytest.raises(ValueError, match="is already published"):
        _authorize_recovery()("0.9.1", revision, "https://pypi.org")


def test_publish_workflow_exposes_only_the_guarded_recovery_inputs() -> None:
    workflow = (
        Path(__file__).parents[1] / ".github" / "workflows" / "publish.yml"
    ).read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "expected_version:" in workflow
    assert "expected_revision:" in workflow
    assert "python scripts/check-publish-recovery.py" in workflow
    assert '--expected-version "${EXPECTED_VERSION}"' in workflow
    assert '--expected-revision "${EXPECTED_REVISION}"' in workflow
