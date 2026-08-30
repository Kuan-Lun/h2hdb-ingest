#!/usr/bin/env python3
"""Authorize one exact manual recovery of an unpublished distribution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from packaging.version import Version

_AUDIT_PATH = Path(".release/dependency-audit.json")
_PACKAGE_PATH = Path("package.json")
_PROJECT_PATH = Path("pyproject.toml")
_PYPI_PROJECT = "h2hdb-ingest"
_RECOVERY_VERSION = Version("0.9.1")
_REVISION_PATTERN = re.compile(r"[0-9a-fA-F]{40,64}")


def _dependency_manifest(
    document: dict[str, Any],
    package: dict[str, Any],
) -> bytes:
    project = document.get("project", {})
    manifest = {
        "build": document.get("build-system", {}).get("requires", []),
        "dependency-groups": document.get("dependency-groups", {}),
        "optional": project.get("optional-dependencies", {}),
        "runtime": project.get("dependencies", []),
        "node-dev": package.get("devDependencies", {}),
    }
    return json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _require_reviewed_audit(
    document: dict[str, Any],
    current: Version,
) -> None:
    receipt = json.loads(_AUDIT_PATH.read_text(encoding="utf-8"))
    package = json.loads(_PACKAGE_PATH.read_text(encoding="utf-8"))
    digest = hashlib.sha256(_dependency_manifest(document, package)).hexdigest()
    if receipt.get("schema") != "h2h.dependency-audit.v1":
        raise ValueError("dependency audit receipt has an unsupported schema")
    if receipt.get("project_version") != str(current):
        raise ValueError("dependency audit receipt does not match project version")
    if receipt.get("manifest_sha256") != digest:
        raise ValueError("dependency audit receipt does not match dependencies")
    review = receipt.get("review", {})
    if review.get("status") != "reviewed" or not review.get("note"):
        raise ValueError("dependency audit receipt lacks a compatibility review")


def _require_unpublished(current: Version, index_url: str) -> None:
    project = urllib.parse.quote(_PYPI_PROJECT, safe="")
    version = urllib.parse.quote(str(current), safe="")
    url = f"{index_url.rstrip('/')}/pypi/{project}/{version}/json"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "h2hdb-ingest-publish-recovery/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return
        raise
    raise ValueError(f"{_PYPI_PROJECT} {current} is already published")


def _authorize_recovery(
    expected_version: str,
    expected_revision: str,
    index_url: str,
) -> Version:
    if _REVISION_PATTERN.fullmatch(expected_revision) is None:
        raise ValueError(f"invalid expected revision: {expected_revision!r}")
    actual_revision = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if actual_revision.lower() != expected_revision.lower():
        raise ValueError(
            "recovery revision mismatch: "
            f"expected {expected_revision}, got {actual_revision}"
        )
    status = subprocess.run(
        ("git", "status", "--porcelain=v1", "--untracked-files=all"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status:
        raise ValueError("recovery checkout is not an exact clean revision")

    requested = Version(expected_version)
    if str(requested) != expected_version:
        raise ValueError("expected version must use its canonical spelling")
    if requested != _RECOVERY_VERSION:
        raise ValueError(
            f"recovery is restricted to {_PYPI_PROJECT} {_RECOVERY_VERSION}"
        )
    document = tomllib.loads(_PROJECT_PATH.read_text(encoding="utf-8"))
    current = Version(str(document["project"]["version"]))
    if current != requested:
        raise ValueError(
            f"recovery version mismatch: expected {requested}, got {current}"
        )
    _require_reviewed_audit(document, current)
    _require_unpublished(current, index_url)
    return current


def _output_path(argument: Path | None) -> Path:
    if argument is not None:
        return argument
    environment_output = os.environ.get("GITHUB_OUTPUT")
    if environment_output is None:
        raise ValueError("GITHUB_OUTPUT or --output is required")
    return Path(environment_output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--index-url", default="https://pypi.org")
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()

    current = _authorize_recovery(
        arguments.expected_version,
        arguments.expected_revision,
        arguments.index_url,
    )
    with _output_path(arguments.output).open("a", encoding="utf-8") as output:
        print(f"current={current}", file=output)
        print("previous=manual-recovery", file=output)
        print("bumped=true", file=output)
    print(f"authorized one-time recovery for {_PYPI_PROJECT} {current}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        KeyError,
        OSError,
        subprocess.CalledProcessError,
        urllib.error.URLError,
        ValueError,
    ) as error:
        print(f"check-publish-recovery: {error}", file=sys.stderr)
        raise SystemExit(1) from error
