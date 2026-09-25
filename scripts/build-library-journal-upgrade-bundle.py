#!/usr/bin/env python3
"""Package an exact checkout wheel and the offline converter for Docker Compose."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[1]
CONVERTER = "upgrade-library-journal-v4-to-v5.py"


def _require_exact_wheel(wheel: Path, *, root: Path, version: str) -> None:
    with zipfile.ZipFile(wheel) as archive:
        metadata = archive.read(f"h2hdb_ingest-{version}.dist-info/METADATA")
        message = BytesParser().parsebytes(metadata)
        if message["Name"] != "h2hdb-ingest" or message["Version"] != version:
            raise ValueError("Wheel distribution differs from the checkout version")
        sources = {
            path.relative_to(root / "src").as_posix(): path.read_bytes()
            for path in (root / "src" / "h2hdb_ingest").rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        }
        installed = {
            name: archive.read(name)
            for name in archive.namelist()
            if name.startswith("h2hdb_ingest/") and not name.endswith("/")
        }
    if installed != sources:
        raise ValueError("Wheel runtime files differ from the checkout")


def _require_core_wheel(core_wheel: Path, ingest_wheel: Path, version: str) -> str:
    with zipfile.ZipFile(core_wheel) as archive:
        names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(names) != 1:
            raise ValueError("Core wheel must contain one distribution metadata record")
        core = BytesParser().parsebytes(archive.read(names[0]))
    if canonicalize_name(str(core["Name"])) != "h2hdb":
        raise ValueError("Core wheel distribution must be h2hdb")
    core_version = str(Version(str(core["Version"])))
    with zipfile.ZipFile(ingest_wheel) as archive:
        ingest = BytesParser().parsebytes(
            archive.read(f"h2hdb_ingest-{version}.dist-info/METADATA")
        )
    requirements = [Requirement(value) for value in ingest.get_all("Requires-Dist", [])]
    bounds = [
        r
        for r in requirements
        if canonicalize_name(r.name) == "h2hdb"
        and (r.marker is None or r.marker.evaluate())
    ]
    if not bounds or not all(Version(core_version) in r.specifier for r in bounds):
        raise ValueError(
            "Core wheel version does not satisfy Ingest wheel dependencies"
        )
    return core_version


def _dockerfile(
    version: str, wheel_name: str, core_name: str, core_version: str
) -> str:
    return f"""FROM python:3.14
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
LABEL io.h2hdb.tool="offline-journal4-to5" io.h2hdb.ingest.version="{version}"
COPY {wheel_name} {core_name} /opt/h2hdb-upgrade/
RUN python -m pip install --no-cache-dir /opt/h2hdb-upgrade/{core_name} /opt/h2hdb-upgrade/{wheel_name} \\
    && python -m pip check \\
    && python -I -c "from importlib.metadata import version; assert version('h2hdb-ingest') == '{version}'; assert version('h2hdb') == '{core_version}'" \\
    && rm /opt/h2hdb-upgrade/{wheel_name} /opt/h2hdb-upgrade/{core_name} \\
    && chmod 1777 /tmp
COPY {CONVERTER} /opt/h2hdb-upgrade/
RUN chmod 0755 /opt /opt/h2hdb-upgrade \\
    && chmod 0444 /opt/h2hdb-upgrade/{CONVERTER}
USER 65534:65534
RUN python -I /opt/h2hdb-upgrade/{CONVERTER} --help >/dev/null
ENTRYPOINT ["python", "-I", "-u", "/opt/h2hdb-upgrade/{CONVERTER}"]
CMD ["--help"]
"""


def _compose(version: str, library_root: str) -> str:
    # JSON quotes YAML syntax; Compose additionally needs $$ for a literal dollar.
    source = json.dumps(library_root.replace("$", "$$"))
    return f"""name: h2hdb-journal5-upgrade
services:
  upgrade:
    build:
      context: .
      dockerfile: Dockerfile
    image: h2hdb-journal5-upgrade:{version}
    user: "${{MEDIA_UID:?set MEDIA_UID in deployment .env}}:${{MEDIA_GID:?set MEDIA_GID in deployment .env}}"
    init: true
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    network_mode: none
    volumes:
      - type: bind
        source: {source}
        target: /hentai/library
        read_only: false
        bind:
          create_host_path: false
      - type: volume
        target: /tmp
    restart: "no"
"""


def build_bundle(
    *,
    root: Path,
    wheel: Path,
    core_wheel: Path,
    output: Path,
    library_root: str,
) -> None:
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    _require_exact_wheel(wheel, root=root, version=version)
    core_version = _require_core_wheel(core_wheel, wheel, version)
    core_name = f"h2hdb-{core_version}-py3-none-any.whl"
    if not Path(library_root).is_absolute():
        raise ValueError("An absolute existing library root is required")
    if output.exists():
        raise FileExistsError(output)
    wheel_name = f"h2hdb_ingest-{version}-py3-none-any.whl"
    files = {
        wheel_name: wheel.read_bytes(),
        core_name: core_wheel.read_bytes(),
        CONVERTER: (root / "scripts" / CONVERTER).read_bytes(),
        "Dockerfile": _dockerfile(
            version, wheel_name, core_name, core_version
        ).encode(),
        "compose.yaml": _compose(version, library_root).encode(),
        ".dockerignore": (
            f"*\n!Dockerfile\n!{wheel_name}\n!{core_name}\n!{CONVERTER}\n"
        ).encode(),
    }
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    provenance = {
        "ingest_version": version,
        "core_version": core_version,
        "checkout_commit": commit,
        "supported_upgrade": "exact journal v4 to v5; atomic conversion and exact v5 replay",
        "files_sha256": {
            name: hashlib.sha256(data).hexdigest() for name, data in files.items()
        },
        "verification": "Every wheel runtime file matches this checkout. Bundle creation does not build an image or access the library.",
    }
    files["provenance.json"] = (json.dumps(provenance, indent=2) + "\n").encode()
    with tempfile.TemporaryDirectory(prefix="h2hdb-upgrade-bundle-") as temporary:
        directory = Path(temporary) / f"h2hdb-journal4-to5-docker-{version}"
        directory.mkdir(mode=0o755)
        directory.chmod(0o755)
        for name, data in files.items():
            path = directory / name
            path.write_bytes(data)
            path.chmod(0o644)
        with (
            output.open("xb") as stream,
            tarfile.open(fileobj=stream, mode="w:gz") as archive,
        ):
            archive.add(directory, arcname=directory.name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--core-wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--library-root", required=True)
    args = parser.parse_args()
    build_bundle(
        root=ROOT,
        wheel=args.wheel,
        core_wheel=args.core_wheel,
        output=args.output,
        library_root=args.library_root,
    )
    print(args.output)


if __name__ == "__main__":
    main()
