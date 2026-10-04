from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from h2hdb import CoreConfig

pytest_plugins = ("backend_contract",)

_MARIADB_IMAGE = "mariadb:10.11.11"
_MARIADB_VERSION_PREFIX = "10.11.11-"
_MARIADB_ROOT_PASSWORD = "h2hdb-ingest-test-root"
_MARIADB_USER = "h2hdb_ingest"
_MARIADB_PASSWORD = "h2hdb-ingest-test-password"
_MARIADB_MAX_ALLOWED_PACKET = 1024 * 1024


@pytest.fixture(scope="session")
def mariadb_container() -> Iterator[Any]:
    if os.environ.get("H2HDB_TEST_MARIADB") != "1":
        pytest.skip("set H2HDB_TEST_MARIADB=1 to run MariaDB integration tests")
    try:
        from testcontainers.community.mysql import MySqlContainer
    except ImportError as error:
        pytest.fail(
            f"MariaDB tests were enabled but dependencies are unavailable: {error}",
            pytrace=False,
        )
    try:
        container = MySqlContainer(
            image=_MARIADB_IMAGE,
            username=_MARIADB_USER,
            password=_MARIADB_PASSWORD,
            root_password=_MARIADB_ROOT_PASSWORD,
            dbname="h2hdb_ingest_template",
            command=f"--max-allowed-packet={_MARIADB_MAX_ALLOWED_PACKET}",
        )
        started = container.start()
    except Exception as error:
        pytest.fail(
            f"MariaDB tests were enabled but the testcontainer is unavailable: {error}",
            pytrace=False,
        )
    try:
        yield started
    finally:
        container.stop()


@contextmanager
def _mariadb_config(mariadb_container: Any) -> Iterator[CoreConfig]:

    from h2hdb import CoreConfig, DatabaseConfig

    try:
        import mysql.connector
    except ImportError as error:
        pytest.fail(
            f"MariaDB tests were enabled but the connector is unavailable: {error}",
            pytrace=False,
        )
    host = mariadb_container.get_container_host_ip()
    port = int(mariadb_container.get_exposed_port(mariadb_container.port))
    database = f"h2hdb_ingest_test_{uuid.uuid4().hex[:12]}"

    admin_connection = mysql.connector.connect(
        host=host,
        port=port,
        user="root",
        password=_MARIADB_ROOT_PASSWORD,
    )
    try:
        with admin_connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version_row = cursor.fetchone()
            assert version_row is not None
            (server_version,) = version_row
            assert str(server_version).startswith(_MARIADB_VERSION_PREFIX)
            cursor.execute(
                f"CREATE DATABASE `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
            )
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON `{database}`.* TO %s",
                (_MARIADB_USER,),
            )
        admin_connection.commit()
    finally:
        admin_connection.close()

    config = CoreConfig(
        database=DatabaseConfig(
            sql_type="mariadb",
            host=host,
            port=port,
            user=_MARIADB_USER,
            password=_MARIADB_PASSWORD,
            database=database,
        )
    )
    try:
        yield config
    finally:
        admin_connection = mysql.connector.connect(
            host=host,
            port=port,
            user="root",
            password=_MARIADB_ROOT_PASSWORD,
        )
        try:
            with admin_connection.cursor() as cursor:
                cursor.execute(f"DROP DATABASE IF EXISTS `{database}`")
            admin_connection.commit()
        finally:
            admin_connection.close()


@pytest.fixture(
    scope="session",
    params=(
        "sqlite",
        pytest.param("mariadb", marks=(pytest.mark.mariadb, pytest.mark.deep)),
    ),
)
def database_backend(request: pytest.FixtureRequest) -> str:
    return cast(str, request.param)


@pytest.fixture(scope="session")
def core_config_factory(
    database_backend: str,
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> Callable[[], AbstractContextManager[CoreConfig]]:
    if database_backend == "mariadb":
        container = request.getfixturevalue("mariadb_container")
        return lambda: _mariadb_config(container)

    @contextmanager
    def sqlite_database() -> Iterator[CoreConfig]:
        from h2hdb import CoreConfig, DatabaseConfig

        path = tmp_path_factory.mktemp("core-sqlite") / "catalog.sqlite3"
        yield CoreConfig(database=DatabaseConfig(sql_type="sqlite", database=str(path)))

    return sqlite_database


@pytest.fixture
def core_config(
    core_config_factory: Callable[[], AbstractContextManager[CoreConfig]],
) -> Iterator[CoreConfig]:
    with core_config_factory() as config:
        yield config


@pytest.fixture
def sqlite_core_config(tmp_path: Path) -> CoreConfig:
    from h2hdb import CoreConfig, DatabaseConfig

    return CoreConfig(
        database=DatabaseConfig(
            sql_type="sqlite", database=str(tmp_path / "catalog.sqlite3")
        )
    )


@pytest.fixture
def probe_core_configs(
    core_config_factory: Callable[[], AbstractContextManager[CoreConfig]],
) -> Iterator[tuple[CoreConfig, ...]]:
    with ExitStack() as stack:
        yield tuple(stack.enter_context(core_config_factory()) for _ in range(4))


@pytest.fixture
def probe_database_input(core_config: CoreConfig) -> str:
    return json.dumps([core_config.model_dump(mode="json")])
