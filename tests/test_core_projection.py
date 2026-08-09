from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path

from h2hdb import H2HDB, CatalogBuildPhase, CoreConfig, DatabaseConfig

from h2hdb_ingest.core_analysis import CoreStagedDeduplicationAdapter
from h2hdb_ingest.core_projection import CoreStagedProjectionAdapter
from h2hdb_ingest.models import (
    CBZArtifact,
    CBZPreparationSummary,
    CBZStreamingPreparationRequest,
)
from h2hdb_ingest.scanner import FilesystemScanner
from h2hdb_ingest.staged_deduplication import StagedDeduplicationPlanner
from h2hdb_ingest.staged_projection import StagedProjectionOrchestrator
from h2hdb_ingest.staging import CoreFileHashCache, FilesystemSourceStager


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _write_gallery(root: Path, name: str, *, title: str, payload: bytes) -> None:
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "galleryinfo.txt").write_text(
        "\n".join(
            (
                f"Title: {title}",
                "Upload Time: 2024-01-02 03:04",
                "Uploaded By: uploader",
                "Downloaded: 2024-01-03 04:05",
                "Tags: artist:tester, language:english",
                "Uploader's Comments:",
                "Summary",
                "Downloaded from E-Hentai Galleries by the Hentai@Home Downloader <3",
            )
        ),
        encoding="utf-8",
    )
    (folder / "001.bin").write_bytes(payload)


class _StreamingArtifactStub:
    def __init__(self, artifact_root: Path) -> None:
        self._artifact_root = artifact_root
        self.protected_page_sizes: list[int] = []
        self.files_seen = 0

    @contextmanager
    def publication_guard(self) -> Iterator[None]:
        yield

    def prepare_paged_stream(
        self,
        requests: Iterable[CBZStreamingPreparationRequest],
        *,
        result_sink: Callable[[CBZArtifact], None] | None = None,
        total: int | None = None,
    ) -> CBZPreparationSummary:
        prepared = 0
        for request in requests:
            for source_file in request.open_files():
                assert source_file.file.path.is_file()
                assert source_file.file.signature is not None
                self.files_seen += 1
            digest = _digest(
                f"artifact:{request.metadata.gallery.gid}:"
                f"{request.metadata.source_digest}"
            )
            artifact = CBZArtifact(
                gallery=request.metadata.gallery,
                path=self._artifact_root / f"{digest}.cbz",
                size_bytes=123,
                sha256=digest,
                modified_at=request.metadata.gallery.upload_time,
                created=True,
                rebuilt=False,
            )
            if result_sink is not None:
                result_sink(artifact)
            prepared += 1
        assert total is None or prepared == total
        return CBZPreparationSummary(prepared, prepared, 0)

    def protect_for_publish(
        self,
        artifacts: Iterable[CBZArtifact],
        *,
        protection_id: str,
    ) -> None:
        assert protection_id
        materialized = tuple(artifacts)
        assert materialized
        self.protected_page_sizes.append(len(materialized))


def test_core_projection_adapter_stages_and_jointly_publishes_bounded_pages(
    tmp_path: Path,
) -> None:
    root = tmp_path / "galleries"
    _write_gallery(root, "First Gallery [101]", title="First", payload=b"first")
    _write_gallery(root, "Second Gallery [102]", title="Second", payload=b"second")
    database = H2HDB(
        CoreConfig(
            database=DatabaseConfig(
                sql_type="sqlite",
                database=str(tmp_path / "catalog.sqlite"),
            )
        )
    )
    database.migrate()
    turn = database.claim_gallery_ingest(lease_seconds=120, periodic_scan=True)
    assert turn is not None

    hash_cache = CoreFileHashCache(database)
    stager = FilesystemSourceStager(
        scanner=FilesystemScanner(
            root,
            hash_workers=2,
            hash_cache=hash_cache,
            max_galleries=1,
            max_files=1,
        ),
        coordinator=database,
        hash_cache=hash_cache,
    )
    build = stager.begin_or_resume(
        scope_key="projection-adapter-v1",
        ingest_turn=turn,
    )
    build = stager.stage(build, ingest_turn=turn)
    assert build.phase is CatalogBuildPhase.analyzing
    StagedDeduplicationPlanner(page_size=1, write_batch_size=1).run(
        CoreStagedDeduplicationAdapter(database, build, turn),
        build_id=build.build_id,
    )
    build = database.complete_catalog_analysis(build, ingest_turn=turn)
    assert build.phase is CatalogBuildPhase.artifacts

    adapter = CoreStagedProjectionAdapter(
        database,
        build,
        turn,
        source_root=root,
    )
    adapter.begin_or_resume(artifacts_required=True)
    selected_page = adapter.page_selected_galleries(
        build.build_id,
        after=None,
        limit=10_000,
    )
    assert len(selected_page.items) == 2
    first = selected_page.items[0]
    assert first.folder.is_dir()
    file_page = adapter.page_selected_gallery_files(
        build.build_id,
        first.gallery_key,
        after=None,
        limit=10_000,
    )
    assert file_page.items
    assert all(item.path.is_file() for item in file_page.items)
    assert all(item.signature.size_bytes == item.size_bytes for item in file_page.items)

    cbz = _StreamingArtifactStub(tmp_path / "artifacts")
    summary = StagedProjectionOrchestrator(
        adapter=adapter,
        cbz=cbz,
        gallery_page_size=2,
        file_page_size=1,
        selection_batch_size=2,
    ).run(build.build_id)
    assert summary.artifacts_prepared == 2
    assert summary.selections_staged == 2
    assert cbz.protected_page_sizes == [2]
    assert cbz.files_seen == 4

    stager.validate(build)
    while True:
        operations = database.prepare_catalog_build_operations(
            build,
            max_rows=1,
            ingest_turn=turn,
        )
        if operations.complete:
            break
    database.seal_catalog_build_projection(build, ingest_turn=turn)
    build = database.seal_catalog_build(build, ingest_turn=turn)
    published = database.publish_catalog_build_with_projection(
        build,
        ingest_turn=turn,
    )
    assert published.receipt.selected_galleries == 2
    assert database.get_catalog_source_revision().revision == 1
    page = database.list_publications(limit=10)
    assert page.total == 2
    assert all(publication.artifacts for publication in page.publications)
