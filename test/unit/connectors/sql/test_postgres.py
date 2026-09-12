import psycopg2
import pytest
from psycopg2 import errorcodes

from unstructured_ingest.error import UserAuthError, UserError
from unstructured_ingest.processes.connectors.sql.postgres import (
    PostgresAccessConfig,
    PostgresConnectionConfig,
    PostgresUploader,
    PostgresUploaderConfig,
)


@pytest.fixture
def uploader() -> PostgresUploader:
    return PostgresUploader(
        connection_config=PostgresConnectionConfig(
            host="localhost",
            port=5432,
            database="elements",
            username="unstructured",
            access_config=PostgresAccessConfig(password="test"),
        ),
        upload_config=PostgresUploaderConfig(table_name="elements"),
    )


def _error(pgcode) -> psycopg2.Error:
    # psycopg2 fills pgcode from libpq and exposes it read-only, so a stand-in has to
    # carry the code on the class the way the driver's own error classes do.
    return type("_PgError", (psycopg2.Error,), {"pgcode": pgcode})("boom")


@pytest.mark.parametrize("privilege", ["INSERT", "DELETE"])
def test_insufficient_privilege_is_a_denial(uploader: PostgresUploader, privilege: str):
    """Postgres uses 42501 for every missing table privilege, so the message has to
    carry the right that was refused -- telling a credential that holds INSERT and not
    DELETE that it lacks INSERT sends the customer to grant the wrong thing."""
    reason = uploader.classify_write_denial(
        _error(errorcodes.INSUFFICIENT_PRIVILEGE), privilege=privilege
    )

    assert reason is not None
    assert f"{privilege} permission on table 'elements'" in reason
    assert f"Grant {privilege} on that table" in reason


def test_read_only_transaction_is_a_denial(uploader: PostgresUploader):
    reason = uploader.classify_write_denial(
        _error(errorcodes.READ_ONLY_SQL_TRANSACTION), privilege="INSERT"
    )

    assert reason is not None
    assert "read-only" in reason


@pytest.mark.parametrize(
    "pgcode",
    [
        # A table that does not exist, or one this credential may not see. Postgres
        # uses the same code for both, so it cannot be called a permissions problem.
        errorcodes.UNDEFINED_TABLE,
        errorcodes.UNDEFINED_COLUMN,
        errorcodes.NOT_NULL_VIOLATION,
        errorcodes.ADMIN_SHUTDOWN,
        None,
    ],
)
def test_other_postgres_errors_are_not_denials(uploader: PostgresUploader, pgcode):
    assert uploader.classify_write_denial(_error(pgcode), privilege="INSERT") is None


def test_a_non_postgres_exception_is_not_a_denial(uploader: PostgresUploader):
    assert uploader.classify_write_denial(TimeoutError("no answer"), privilege="DELETE") is None


def test_a_denial_is_a_user_error_and_not_an_auth_error(uploader: PostgresUploader):
    reason = uploader.classify_write_denial(
        _error(errorcodes.INSUFFICIENT_PRIVILEGE), privilege="INSERT"
    )
    error = UserError(reason)

    assert error.status_code == 422
    assert not isinstance(error, UserAuthError)
