import argparse
from collections.abc import Sequence

from h2hdb import H2HDB

from .cbz import CBZReconciler
from .config import load_config
from .deduplication import DeduplicationPolicy
from .resident import ResidentIngestor
from .scanner import FilesystemScanner
from .service import IngestService


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Scan galleries and publish H2HDB revisions"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    config.ensure_paths()
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
    service = IngestService(
        scanner=FilesystemScanner(
            config.paths.download_path,
            hash_workers=config.paths.hash_workers,
            event_logger=database.logger.info,
        ),
        deduplication=DeduplicationPolicy(),
        cbz=cbz,
        catalog_reader=database,
        catalog_publisher=database,
        database_admin=database,
        sort_mode=config.paths.cbz_sort,
    )
    resident = ResidentIngestor(
        service=service,
        coordinator=database,
        database_admin=database,
        config=config.resident,
        database_type=config.core.database.sql_type,
        event_logger=database.logger.info,
    )
    resident.initialize()
    if args.once:
        if not resident.process_available(periodic_scan=True):
            raise RuntimeError("No gallery ingest lease is currently available")
    else:
        resident.run_forever()


if __name__ == "__main__":
    main()
