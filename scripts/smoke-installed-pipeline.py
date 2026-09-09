"""Offline installed-wheel gallery -> catalog/CBZ smoke, optionally through OPDS.

This script imports production APIs only. It never imports pytest fixtures,
searches sibling checkouts, reads a private corpus, or opens a network socket.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import struct
import sys
import zlib
from contextlib import closing
from dataclasses import dataclass
from hashlib import sha256
from importlib.metadata import distribution, version
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from zipfile import ZipFile

from h2hdb import CatalogTagFilter, CoreConfig, DatabaseConfig, open_database
from PIL import Image

from h2hdb_ingest import IngestConfig, IngestPathsConfig, ResidentConfig
from h2hdb_ingest.runtime import IngestRuntime, build_runtime


@dataclass(frozen=True)
class _PublishedBytes:
    gid: int
    archive: bytes
    thumbnail: bytes


def _wheel_origin(module_name: str, project: str, *, allow_external: bool) -> str:
    module = importlib.import_module(module_name)
    package = distribution(project)
    assert module.__file__ is not None
    actual = Path(module.__file__).resolve()
    expected = Path(str(package.locate_file(module_name + "/__init__.py"))).resolve()
    assert actual == expected, f"{project} loaded outside its installed wheel: {actual}"
    files = package.files
    assert files is not None
    assert any(str(item) == module_name + "/__init__.py" for item in files), (
        f"{project} has no installed package record; editable/source imports are forbidden"
    )
    direct = json.loads(package.read_text("direct_url.json") or "{}")
    assert not direct.get("dir_info", {}).get("editable", False), (
        f"{project} is editable, not an installed candidate wheel"
    )
    if not allow_external:
        assert actual.is_relative_to(Path(sys.prefix).resolve()), (
            f"{project} must be installed in the smoke environment: {actual}"
        )
    print(
        json.dumps(
            {
                "project": project,
                "version": package.version,
                "wheel_origin": str(actual),
            }
        ),
        flush=True,
    )
    return str(actual)


def _gallery(
    root: Path, gid: int, *, color: str | None, long_page: bool = False
) -> Path:
    folder = root / str(gid)
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        f"Title: Installed wheel fixture {gid}\n"
        "Upload Time: 2024-01-02 03:04\n"
        "Uploaded By: uploader\n"
        "Downloaded: 2024-02-03 04:05\n"
        "Tags: artist:shared, group:shared, language:english\n"
        "Uploader's Comments\n"
        "Offline installed distribution acceptance fixture\n"
        "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3\n",
        encoding="utf-8",
    )
    page = folder / "001.png"
    if color is None:
        page.write_bytes(b"deliberately invalid image bytes")
    else:
        with Image.new("RGB", (10, 10_000) if long_page else (20, 30), color) as image:
            image.save(page, format="PNG")
    return folder


def _write_large_source_png(path: Path) -> int:
    """Generate a valid >32 MiB PNG as rows, without padding or a full raster."""
    width, height = 4096, 2816

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    compressor = zlib.compressobj(level=0)
    with path.open("wb") as stream:
        stream.write(b"\x89PNG\r\n\x1a\n")
        stream.write(
            chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        )
        row = b"\0" + bytes((20, 80, 140)) * width
        for _ in range(height):
            encoded = compressor.compress(row)
            if encoded:
                stream.write(chunk(b"IDAT", encoded))
        stream.write(chunk(b"IDAT", compressor.flush()))
        stream.write(chunk(b"IEND", b""))
    size = path.stat().st_size
    assert size > 32 * 1024 * 1024
    return size


def _inspect(
    runtime: IngestRuntime, library: Path, expected: set[int]
) -> tuple[_PublishedBytes, ...]:
    page = runtime.catalog.discover_publications()
    assert page.total == len(expected)
    assert {publication.gid for publication in page.publications} == expected
    current = library / "current"
    assert len(tuple(current.rglob("*.cbz"))) == len(expected)
    result: list[_PublishedBytes] = []
    for publication in page.publications:
        assert publication.page_count == 1
        assert len(publication.artifacts) == 1
        assert publication.cover is not None
        assert publication.thumbnail is not None
        artifact = publication.artifacts[0].storage_object
        thumbnail = publication.thumbnail.storage_object
        archive_bytes = current.joinpath(*artifact.key.segments).read_bytes()
        thumbnail_bytes = current.joinpath(*thumbnail.key.segments).read_bytes()
        assert sha256(archive_bytes).hexdigest() == artifact.sha256
        assert sha256(thumbnail_bytes).hexdigest() == thumbnail.sha256
        with ZipFile(BytesIO(archive_bytes)) as archive:
            assert archive.namelist() == ["galleryinfo.txt", "pages/0000.jpg"]
            assert archive.testzip() is None
            with Image.open(BytesIO(archive.read("pages/0000.jpg"))) as image:
                image.load()
                assert image.format == "JPEG"
                assert max(image.size) <= 8192
                if publication.gid == 2002:
                    assert max(image.size) > 768
        with Image.open(BytesIO(thumbnail_bytes)) as image:
            image.load()
            assert image.format == "JPEG"
            assert max(image.size) <= 320
        cover = publication.cover
        assert (
            sha256(
                archive_bytes[
                    cover.extent.offset : cover.extent.offset + cover.extent.length
                ]
            ).hexdigest()
            == cover.sha256
        )
        result.append(_PublishedBytes(publication.gid, archive_bytes, thumbnail_bytes))
    for namespace in ("artist", "group"):
        tags = runtime.catalog.list_tag_values(namespace=namespace)
        assert [tag.value for tag in tags.values] == ["shared"]
        tagged = runtime.catalog.list_tag_publications(
            subject=CatalogTagFilter(namespace, "shared")
        )
        assert {publication.gid for publication in tagged.publications} == expected
    assert runtime.database_admin.check().state == "READY"
    assert not (library / ".h2hdb-coordination" / "ACTIVATING").exists()
    return tuple(result)


async def _opds_probe(
    core: CoreConfig, library: Path, expected: tuple[_PublishedBytes, ...]
) -> None:
    # Optional imports keep the default release gate independent of OPDS/HTTPX.
    opds = importlib.import_module("h2hdb_opds")
    httpx = importlib.import_module("httpx")
    config = opds.OPDSConfig(
        core=core,
        library_root=library / "current",
        coordination_root=library / ".h2hdb-coordination",
        public_base_url="http://smoke.invalid",
    )
    with closing(open_database(config.core)) as reader:
        application = opds.create_app(config, catalog=reader)
        async with application.router.lifespan_context(application):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application),
                base_url="http://smoke.invalid",
            ) as client:
                response = await client.get("/opds/v2/publications")
                assert response.status_code == 200
                publications: list[dict[str, Any]] = response.json()["publications"]
                by_id = {item["metadata"]["identifier"]: item for item in publications}
                assert set(by_id) == {
                    f"urn:h2h:gallery:{item.gid}" for item in expected
                }
                for item in expected:
                    publication = by_id[f"urn:h2h:gallery:{item.gid}"]
                    acquisition = next(
                        link
                        for link in publication["links"]
                        if str(link.get("rel", "")).startswith(
                            "http://opds-spec.org/acquisition"
                        )
                    )
                    downloaded = await client.get(acquisition["href"])
                    assert downloaded.status_code == 200
                    assert downloaded.content == item.archive
                    partial = await client.get(
                        acquisition["href"], headers={"Range": "bytes=2-9"}
                    )
                    assert partial.status_code == 206
                    assert partial.content == item.archive[2:10]
                    thumbnail = next(
                        link
                        for link in publication["images"]
                        if link.get("rel") == "http://opds-spec.org/image/thumbnail"
                    )
                    response = await client.get(thumbnail["href"])
                    assert response.status_code == 200
                    assert response.content == item.thumbnail


def _run_pipeline(*, with_opds: bool) -> None:
    with TemporaryDirectory(prefix="h2hdb-installed-pipeline-") as temporary:
        root = Path(temporary).resolve()
        source, library = root / "download", root / "library"
        _gallery(source, 2001, color="red")
        _gallery(source, 2002, color="green", long_page=True)
        repair = _gallery(source, 2003, color=None)
        large = _gallery(source, 2004, color="purple")
        source_size = _write_large_source_png(large / "001.png")
        for relative in (
            "current/acquisitions",
            "current/artwork",
            ".h2hdb-coordination",
        ):
            (library / relative).mkdir(parents=True, exist_ok=True)
        config = IngestConfig(
            core=CoreConfig(
                database=DatabaseConfig(
                    sql_type="sqlite", database=str(root / "catalog.sqlite3")
                )
            ),
            paths=IngestPathsConfig(
                download_path=source, library_path=library, page_render_workers=2
            ),
            resident=ResidentConfig(
                publication_batch_galleries=10, lease_seconds=30, heartbeat_seconds=5
            ),
        )
        with build_runtime(config) as runtime:
            runtime.database_admin.initialize()
            runtime.resident.initialize()
            assert runtime.resident.process_available(periodic_scan=True)
            published = _inspect(runtime, library, {2001, 2002, 2004})
            first_revision = runtime.catalog.discover_publications().revision.revision
            if with_opds:
                asyncio.run(_opds_probe(config.core, library, published))
        # Re-open the real database and library before source repair. Rejection
        # must survive process-style runtime replacement without losing good CBZs.
        with build_runtime(config) as runtime:
            runtime.resident.initialize()
            assert runtime.resident.process_available(periodic_scan=True)
            _inspect(runtime, library, {2001, 2002, 2004})
            with Image.new("RGB", (20, 30), "blue") as image:
                image.save(repair / "001.png", format="PNG")
            marker = repair / "galleryinfo.txt"
            previous = marker.stat()
            marker.write_text(
                marker.read_text().replace("04:05", "04:06"), encoding="utf-8"
            )
            os.utime(
                marker, ns=(previous.st_atime_ns, previous.st_mtime_ns + 1_000_000)
            )
            for _attempt in range(32):
                runtime.resident.process_available(periodic_scan=True)
                if runtime.catalog.discover_publications().total == 4:
                    break
            else:
                raise AssertionError(
                    "repaired gallery did not publish within 32 bounded polls"
                )
            published = _inspect(runtime, library, {2001, 2002, 2003, 2004})
            final_revision = runtime.catalog.discover_publications().revision.revision
            assert final_revision > first_revision
            if with_opds:
                asyncio.run(_opds_probe(config.core, library, published))
        print(
            json.dumps(
                {
                    "pipeline_behavior": "passed",
                    "initial_publications": 3,
                    "repaired_publications": 4,
                    "large_source_encoded_bytes": source_size,
                    "first_revision": first_revision,
                    "final_revision": final_revision,
                    "opds_http": "passed" if with_opds else "not requested",
                }
            ),
            flush=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--opds",
        action="store_true",
        help="also validate installed OPDS and HTTPX through in-process ASGI",
    )
    parser.add_argument(
        "--allow-external-core-wheel",
        action="store_true",
        help="allow an installed core wheel from the explicitly shared development site",
    )
    arguments = parser.parse_args()
    assert not sys.flags.optimize, "smoke assertions must not be disabled with -O"
    assert sys.flags.isolated, "run the installed-wheel smoke with python -I"
    _wheel_origin("h2hdb_ingest", "h2hdb-ingest", allow_external=False)
    _wheel_origin("h2hdb", "h2hdb", allow_external=arguments.allow_external_core_wheel)
    dependency_errors: list[str] = []
    if arguments.opds:
        _wheel_origin("h2hdb_opds", "h2hdb-opds", allow_external=False)
        from packaging.requirements import Requirement

        for raw in distribution("h2hdb-opds").requires or ():
            requirement = Requirement(raw)
            if requirement.name == "h2hdb":
                if not requirement.specifier.contains(version("h2hdb")):
                    dependency_errors.append(
                        f"h2hdb-opds requires {requirement}; installed core is {version('h2hdb')}. "
                        "The installed three-project combination is not supported."
                    )
    if dependency_errors:
        print(
            json.dumps(
                {"dependency_constraints": "failed", "reasons": dependency_errors}
            ),
            flush=True,
        )
    _run_pipeline(with_opds=arguments.opds)
    if dependency_errors:
        raise RuntimeError(
            "HTTP diagnostics completed, but the installed dependency combination remains unsupported: "
            + "; ".join(dependency_errors)
        )


if __name__ == "__main__":
    main()
