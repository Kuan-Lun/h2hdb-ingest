import argparse
from collections.abc import Sequence

from h2hdb import H2HDB

from .cbz import CBZReconciler
from .config import IngestConfig, load_config
from .resident import ResidentIngestor
from .scanner import FilesystemScanner
from .scope import catalog_scope_key
from .staged_deduplication import StagedDeduplicationPlanner
from .staged_service import StagedIngestService
from .staging import CoreFileHashCache, FilesystemSourceStager


def _build_runtime(config: IngestConfig) -> tuple[H2HDB, ResidentIngestor]:
    """Construct the one production staged runtime used by both entry points."""

    if config.paths.cbz_path is not None and config.paths.cbz_sort != "no":
        raise ValueError(
            "The staged ingest pipeline currently supports only cbz_sort='no'; "
            "refusing to silently publish a differently ordered projection"
        )
    database = H2HDB(config.core)
    cbz = None
    if config.paths.cbz_path is not None:
        assert config.paths.artifact_store_path is not None
        cbz = CBZReconciler(
            artifact_store_path=config.paths.artifact_store_path,
            cbz_path=config.paths.cbz_path,
            max_image_short_side=config.paths.max_image_short_side,
            grouping=config.paths.cbz_grouping,
            workers=config.paths.cbz_workers,
            stale_temp_age_seconds=config.paths.stale_temp_age_seconds,
            event_logger=database.logger.info,
        )
    hash_cache = CoreFileHashCache(database)
    scanner = FilesystemScanner(
        config.paths.download_path,
        hash_workers=config.paths.hash_workers,
        hash_cache=hash_cache,
        max_galleries=config.paths.scan_batch_galleries,
        max_files=config.paths.scan_batch_files,
        event_logger=database.logger.info,
    )
    service = StagedIngestService(
        source_stager=FilesystemSourceStager(
            scanner=scanner,
            coordinator=database,
            hash_cache=hash_cache,
        ),
        planner=StagedDeduplicationPlanner(),
        catalog=database,
        database_admin=database,
        catalog_reader=database,
        source_root=config.paths.download_path,
        scope_key=catalog_scope_key(config.paths),
        cbz=cbz,
        event_logger=database.logger.info,
    )
    resident = ResidentIngestor(
        service=service,
        coordinator=database,
        database_admin=database,
        config=config.resident,
        database_type=config.core.database.sql_type,
        event_logger=database.logger.info,
    )
    return database, resident


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Scan galleries and publish H2HDB revisions"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    config.ensure_paths()
    _database, resident = _build_runtime(config)
    resident.initialize()
    if args.once:
        if not resident.process_available(periodic_scan=True):
            raise RuntimeError("No gallery ingest lease is currently available")
    else:
        resident.run_forever()


if __name__ == "__main__":
    main()
