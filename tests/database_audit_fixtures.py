"""Observer/coordination unit-test isolation; real scheduling has separate tests."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from h2hdb import DatabaseAuditReport, VNextDatabaseAdminFacade

from h2hdb_ingest import ResidentConfig


class IsolatedDatabaseAudit:
    """Keep coordination unit tests independent of the separately tested scheduler."""

    def __init__(
        self,
        admin: VNextDatabaseAdminFacade,
        _config: ResidentConfig,
        _emit: Callable[[str], None],
        *,
        database_type: str = "sqlite",
    ) -> None:
        del database_type
        self._admin = admin

    def start(
        self, *, should_stop: Callable[[], bool] | None = None
    ) -> DatabaseAuditReport:
        del should_stop
        return cast(DatabaseAuditReport, self._admin.check())

    def check_between_sessions(self) -> None:
        pass

    def initial_catchup_complete(self) -> None:
        pass

    def raise_if_failed(self) -> None:
        pass

    def fail(self, _error: BaseException) -> None:
        pass
