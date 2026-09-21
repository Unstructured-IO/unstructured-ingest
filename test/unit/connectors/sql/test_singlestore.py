import pytest

from unstructured_ingest.processes.connectors.sql.singlestore import (
    SingleStoreAccessConfig,
    SingleStoreConnectionConfig,
    SingleStoreUploader,
    SingleStoreUploaderConfig,
)


class _FakeSingleStoreError(Exception):
    """Stand-in for singlestoredb.exceptions.OperationalError.

    `singlestoredb` is the `singlestore` extra, not part of the base `test` dependency
    group, so importing it at module scope fails the whole file at collection wherever the
    extra is not installed. The classifier reads one thing, `getattr(error, "errno", None)`,
    and never the type, so carrying `errno` is the whole contract and these cases exercise
    the real classifier rather than skipping. `test_the_driver_error_carries_errno` pins
    that contract against the real driver wherever it is installed.
    """

    def __init__(self, errno=None, msg=""):
        super().__init__(msg)
        self.errno = errno


def test_the_driver_error_carries_errno():
    """The fake above stands in for this. If singlestoredb ever stopped exposing `errno`,
    every test in this file would keep passing against the fake while the classifier went
    blind in production, so the real driver's contract is asserted here."""
    exceptions = pytest.importorskip(
        "singlestoredb.exceptions", reason="singlestoredb is the singlestore extra"
    )

    error = exceptions.OperationalError(errno=1142, msg="INSERT command denied to user")

    assert error.errno == 1142


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
    error = _FakeSingleStoreError(errno=errno, msg=f"{privilege} command denied to user")

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
    error = _FakeSingleStoreError(errno=errno, msg="x")
    assert uploader.classify_write_denial(error, privilege="INSERT") is None


def test_a_non_driver_exception_is_not_a_denial(uploader: SingleStoreUploader):
    assert uploader.classify_write_denial(TimeoutError("no answer"), privilege="DELETE") is None
