import pytest
from singlestoredb.exceptions import OperationalError

from unstructured_ingest.processes.connectors.sql.singlestore import (
    SingleStoreAccessConfig,
    SingleStoreConnectionConfig,
    SingleStoreUploader,
    SingleStoreUploaderConfig,
)


@pytest.fixture
def uploader() -> SingleStoreUploader:
    return SingleStoreUploader(
        connection_config=SingleStoreConnectionConfig(
            host="localhost",
            port=3306,
            user="unstructured",
            database="ingest_test",
            access_config=SingleStoreAccessConfig(password="test"),
        ),
        upload_config=SingleStoreUploaderConfig(table_name="elements"),
    )


@pytest.mark.parametrize("errno", [1044, 1142, 1143])
@pytest.mark.parametrize("privilege", ["INSERT", "DELETE"])
def test_access_denied_errnos_are_denials(
    uploader: SingleStoreUploader, errno: int, privilege: str
):
    """1142 carries the refused command in its own text and arrives for both, so the
    message's privilege has to come from the probe, not from the error number."""
    error = OperationalError(errno=errno, msg=f"{privilege} command denied to user")

    reason = uploader.classify_write_denial(error, privilege=privilege)

    assert reason is not None
    assert f"{privilege} permission on table 'elements'" in reason


@pytest.mark.parametrize(
    "errno",
    [
        # A rejected password is authentication, not a missing grant, and it fails at
        # connect long before the probe runs.
        1045,
        # Unknown table: SingleStore distinguishes this from a denial, so we do too.
        1146,
        1064,
        None,
    ],
)
def test_other_errnos_are_not_denials(uploader: SingleStoreUploader, errno):
    error = OperationalError(errno=errno, msg="x")
    assert uploader.classify_write_denial(error, privilege="INSERT") is None


def test_a_non_driver_exception_is_not_a_denial(uploader: SingleStoreUploader):
    assert uploader.classify_write_denial(TimeoutError("no answer"), privilege="DELETE") is None
