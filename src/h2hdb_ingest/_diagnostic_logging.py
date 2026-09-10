"""Process targets for warning/error output without changing logger records."""

from collections.abc import Iterator
from contextlib import contextmanager
from copy import copy
from logging import WARNING, Formatter, LogRecord
from pathlib import Path

from ._log_fields import quote_log_field
from .config import IngestConfig


def diagnostic_targets(config: IngestConfig) -> str:
    """Identify configured resources; deliberately omit credentials."""

    paths = config.paths
    database = config.core.database
    fields = [
        "source_root=" + quote_log_field(str(paths.download_path.absolute())),
        "library_root="
        + (
            "null"
            if paths.library_path is None
            else quote_log_field(str(paths.library_path.absolute()))
        ),
        "database_backend=" + quote_log_field(database.sql_type),
    ]
    if database.sql_type == "sqlite":
        fields.append(
            "database_path="
            + quote_log_field(
                database.database
                if database.database == ":memory:"
                else str(Path(database.database).absolute())
            )
        )
    else:
        fields.extend(
            (
                "database_host=" + quote_log_field(database.host),
                f"database_port={database.port}",
                "database_name=" + quote_log_field(database.database),
            )
        )
    return " ".join(fields)


class DiagnosticFormatter(Formatter):
    def __init__(self, config: IngestConfig) -> None:
        super().__init__("%(asctime)s [%(levelname)s] %(message)s")
        self._targets = diagnostic_targets(config)

    def format(self, record: LogRecord) -> str:
        if record.levelno < WARNING:
            return super().format(record)
        # Console/file handlers may format the same record independently. Never
        # mutate it or append context twice, including under parallel logging.
        contextual = copy(record)
        contextual.msg = (
            record.getMessage()
            + " | logger="
            + quote_log_field(record.name)
            + " thread="
            + quote_log_field(record.threadName or "unknown")
            + " "
            + self._targets
        )
        contextual.args = ()
        return super().format(contextual)


@contextmanager
def command_diagnostics(
    config_path: str | Path, config: IngestConfig | None = None
) -> Iterator[None]:
    """Keep fatal CLI tracebacks locatable, preserving exceptions and exit codes."""

    try:
        yield
    except Exception as error:
        context = "config_file=" + quote_log_field(str(Path(config_path).absolute()))
        if config is not None:
            context += " " + diagnostic_targets(config)
        error.add_note("Ingest command context: " + context)
        raise
