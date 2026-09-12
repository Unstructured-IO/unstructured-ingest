import sqlite3

import pytest

from unstructured_ingest.processes.connectors.sql.sqlite import (
    SQLiteConnectionConfig,
    SQLiteUploader,
    SQLiteUploaderConfig,
)


@pytest.fixture
def uploader(tmp_path) -> SQLiteUploader:
    database_path = tmp_path / "elements.db"
    sqlite3.connect(database_path).close()
    return SQLiteUploader(
        connection_config=SQLiteConnectionConfig(database_path=database_path),
        upload_config=SQLiteUploaderConfig(table_name="elements"),
    )


def _error(code: int) -> sqlite3.OperationalError:
    error = sqlite3.OperationalError("attempt to write a readonly database")
    error.sqlite_errorcode = code
    return error


@pytest.mark.parametrize(
    "code",
    [
        sqlite3.SQLITE_READONLY,
        # Every SQLITE_READONLY_* extended code carries the primary one in its low byte.
        sqlite3.SQLITE_READONLY_RECOVERY,
        sqlite3.SQLITE_READONLY_CANTLOCK,
        sqlite3.SQLITE_READONLY_ROLLBACK,
        sqlite3.SQLITE_READONLY_DBMOVED,
        sqlite3.SQLITE_READONLY_CANTINIT,
        sqlite3.SQLITE_READONLY_DIRECTORY,
    ],
)
@pytest.mark.parametrize("privilege", ["INSERT", "DELETE"])
def test_readonly_codes_are_denials(uploader: SQLiteUploader, code: int, privilege: str):
    # A read-only file refuses the INSERT and the DELETE alike; SQLite has no grants,
    # so the message names the file rather than a privilege.
    reason = uploader.classify_write_denial(_error(code), privilege=privilege)

    assert reason is not None
    assert "read-only" in reason


@pytest.mark.parametrize(
    "code", [sqlite3.SQLITE_ERROR, sqlite3.SQLITE_BUSY, sqlite3.SQLITE_CANTOPEN, None]
)
def test_other_sqlite_codes_are_not_denials(uploader: SQLiteUploader, code):
    assert uploader.classify_write_denial(_error(code), privilege="INSERT") is None


def test_an_exception_without_a_sqlite_code_is_not_a_denial(uploader: SQLiteUploader):
    assert uploader.classify_write_denial(TimeoutError("no answer"), privilege="DELETE") is None
