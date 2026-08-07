__all__ = ["IngestService"]

from h2hdb import (
    CatalogArtifact,
    CatalogPublication,
    CatalogPublicationSelection,
    CatalogPublisher,
    CatalogReader,
    CatalogRevision,
    CatalogSnapshot,
    DatabaseAdmin,
    GalleryIngestTurn,
    GallerySourceFile,
    GallerySourceRecord,
    GalleryTag,
)

from .cbz import CBZReconciler
from .deduplication import DeduplicationPolicy
from .models import CBZArtifact, DeduplicationPlan, ScannedGallery, SyncOutcome
from .naming import gallery_name_to_cbz_file_name
from .scanner import FilesystemScanner


class IngestService:
    def __init__(
        self,
        *,
        scanner: FilesystemScanner,
        deduplication: DeduplicationPolicy,
        cbz: CBZReconciler | None,
        catalog_reader: CatalogReader,
        catalog_publisher: CatalogPublisher,
        database_admin: DatabaseAdmin,
        sort_mode: str = "no",
    ) -> None:
        self._scanner = scanner
        self._deduplication = deduplication
        self._cbz = cbz
        self._catalog_reader = catalog_reader
        self._catalog_publisher = catalog_publisher
        self._database_admin = database_admin
        self._sort_mode = sort_mode

    def synchronize_once(self, turn: GalleryIngestTurn) -> SyncOutcome:
        _, current = self._read_current_snapshot()
        incumbent_gallery_name_by_content_sha256 = {
            publication.content_sha256: publication.source_gallery_name
            for publication in current.values()
            if publication.content_sha256 is not None
        }
        incumbent_gallery_name_by_gid = {
            publication.gid: publication.source_gallery_name
            for publication in current.values()
        }
        scanned = self._scanner.scan()
        plan = self._deduplication.select(
            scanned,
            incumbent_gallery_name_by_content_sha256=(
                incumbent_gallery_name_by_content_sha256
            ),
            incumbent_gallery_name_by_gid=incumbent_gallery_name_by_gid,
        )
        plan = DeduplicationPlan(
            canonical_galleries=plan.canonical_galleries,
            winners=self._sort_galleries(plan.winners),
            losers=plan.losers,
            duplicate_of_by_gallery_name=plan.duplicate_of_by_gallery_name,
            excluded_file_sha256s=plan.excluded_file_sha256s,
        )
        artifacts = self._cbz.prepare(plan) if self._cbz is not None else ()
        if self._cbz is not None:
            self._cbz.protect_for_publish(artifacts)
        artifact_by_gid = {artifact.gallery.gid: artifact for artifact in artifacts}
        selections = tuple(
            self._to_publication_selection(
                gallery,
                artifact_by_gid.get(gallery.gid),
            )
            for gallery in plan.winners
        )
        duplicate_of_by_name = dict(plan.duplicate_of_by_gallery_name)
        snapshot = CatalogSnapshot(
            galleries=tuple(
                self._to_source_record(
                    gallery,
                    duplicate_of_gallery_name=duplicate_of_by_name.get(
                        gallery.gallery_name
                    ),
                )
                for gallery in plan.canonical_galleries
            ),
            selections=selections,
        )
        if self._cbz is None:
            with self._database_admin.database_gate():
                publish_result = self._catalog_publisher.publish_snapshot(
                    snapshot,
                    ingest_turn=turn,
                )
        else:
            # This lock order is deliberate and global: publication flock first,
            # database gate second.  Holding the shared artifact-store flock from
            # before publication through projection finalization closes the
            # cross-filesystem revision-check TOCTOU window.
            with self._cbz.publication_guard():
                with self._database_admin.database_gate():
                    publish_result = self._catalog_publisher.publish_snapshot(
                        snapshot,
                        ingest_turn=turn,
                    )
                latest_revision = self._catalog_reader.get_catalog_revision()
                if latest_revision.revision != publish_result.revision.revision:
                    raise RuntimeError(
                        "Catalog revision advanced before Komga projection finalize: "
                        f"expected {publish_result.revision.revision}, "
                        f"found {latest_revision.revision}"
                    )
                self._cbz.finalize_published(
                    artifacts,
                    revision=publish_result.revision.revision,
                )
                finalized_revision = self._catalog_reader.get_catalog_revision()
                if finalized_revision.revision != publish_result.revision.revision:
                    raise RuntimeError(
                        "Catalog revision advanced while finalizing the Komga "
                        "projection: "
                        f"expected {publish_result.revision.revision}, "
                        f"found {finalized_revision.revision}"
                    )
        with self._database_admin.database_gate():
            self._database_admin.record_catalog_changes(
                changed=publish_result.changed_galleries,
                removed=publish_result.removed_galleries,
            )

        return SyncOutcome(
            revision=publish_result.revision.revision,
            scanned=len(scanned),
            published=len(selections),
            new=publish_result.new_galleries,
            changed=publish_result.changed_galleries,
            removed=publish_result.removed_galleries,
            duplicate_losers=len(plan.losers),
            cbz_created=sum(artifact.created for artifact in artifacts),
            cbz_rebuilt=sum(artifact.rebuilt for artifact in artifacts),
        )

    @staticmethod
    def _to_source_record(
        gallery: ScannedGallery,
        *,
        duplicate_of_gallery_name: str | None,
    ) -> GallerySourceRecord:
        tags = tuple(
            GalleryTag(name, value) for name, value in dict.fromkeys(gallery.tags)
        )
        files = tuple(
            GallerySourceFile(
                name=file.name,
                size_bytes=file.size_bytes,
                sha256=file.sha256,
            )
            for file in gallery.files
        )
        return GallerySourceRecord(
            gallery_name=gallery.gallery_name,
            gid=gallery.gid,
            title=gallery.title,
            comment=gallery.summary,
            upload_account=gallery.upload_account,
            upload_time=gallery.upload_time,
            download_time=gallery.download_time,
            modified_time=gallery.modified_time,
            tags=tags,
            files=files,
            source_manifest_sha256=gallery.source_digest,
            content_sha256=gallery.content_digest,
            duplicate_of_gallery_name=duplicate_of_gallery_name,
        )

    def _read_current_snapshot(
        self,
    ) -> tuple[CatalogRevision, dict[str, CatalogPublication]]:
        revision = self._catalog_reader.get_catalog_revision()
        publications: list[CatalogPublication] = []
        offset = 0
        while True:
            page = self._catalog_reader.list_publications(
                offset=offset,
                limit=200,
                revision=revision,
            )
            publications.extend(page.publications)
            offset += len(page.publications)
            if offset >= page.total or not page.publications:
                break
        return revision, {
            publication.publication_id: publication for publication in publications
        }

    def _sort_galleries(
        self,
        galleries: tuple[ScannedGallery, ...],
    ) -> tuple[ScannedGallery, ...]:
        if self._sort_mode == "no":
            return galleries
        if self._sort_mode == "upload_time":
            return tuple(
                sorted(galleries, key=lambda gallery: gallery.upload_time, reverse=True)
            )
        if self._sort_mode == "download_time":
            return tuple(
                sorted(
                    galleries,
                    key=lambda gallery: gallery.download_time,
                    reverse=True,
                )
            )
        if self._sort_mode == "gid":
            return tuple(
                sorted(galleries, key=lambda gallery: gallery.gid, reverse=True)
            )
        if self._sort_mode == "title":
            return tuple(
                sorted(galleries, key=lambda gallery: gallery.title, reverse=True)
            )
        if self._sort_mode == "pages":
            target = 20
        elif self._sort_mode.startswith("pages+"):
            target = max(1, int(self._sort_mode.partition("+")[2]))
        else:
            raise ValueError(f"Unsupported CBZ sort mode: {self._sort_mode}")
        return tuple(sorted(galleries, key=lambda gallery: abs(gallery.pages - target)))

    @staticmethod
    def _to_publication_selection(
        gallery: ScannedGallery,
        artifact: CBZArtifact | None,
    ) -> CatalogPublicationSelection:
        catalog_artifacts = (
            (
                CatalogArtifact(
                    artifact_id=f"urn:h2h:artifact:cbz:{gallery.gid}",
                    name=gallery_name_to_cbz_file_name(gallery.gallery_name),
                    location=artifact.path,
                    media_type="application/vnd.comicbook+zip",
                    size_bytes=artifact.size_bytes,
                    sha256=artifact.sha256,
                    modified_at=artifact.modified_at,
                ),
            )
            if artifact is not None
            else ()
        )
        return CatalogPublicationSelection(
            source_gallery_name=gallery.gallery_name,
            artifacts=catalog_artifacts,
        )
