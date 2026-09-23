import logging
import traceback

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.error import ProviderError, RateLimitError, UserAuthError, UserError
from unstructured_ingest.processes.connectors.databricks.volumes_native import (
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
    DatabricksNativeVolumesIndexer,
    DatabricksNativeVolumesIndexerConfig,
    DatabricksNativeVolumesUploader,
    DatabricksNativeVolumesUploaderConfig,
)
from unstructured_ingest.utils.string_and_date_utils import parse_timestamp

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _connection_config() -> DatabricksNativeVolumesConnectionConfig:
    return DatabricksNativeVolumesConnectionConfig(
        access_config=DatabricksNativeVolumesAccessConfig(token=SECRET),
        host="https://example.databricks.com",
    )


def test_wrap_error_value_auth_redacts():
    pytest.importorskip("databricks.sdk")
    config = _connection_config()
    wrapped = config.wrap_error(ValueError(f"auth: {SECRET}"))

    assert isinstance(wrapped, UserAuthError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_databricks_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[403]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, UserAuthError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_provider_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[500]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, ProviderError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_user_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[400]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, UserError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


@pytest.mark.parametrize(
    ("error_name", "expected"),
    [
        ("ResourceDoesNotExist", UserError),
        ("InvalidParameterValue", UserError),
        ("RequestLimitExceeded", RateLimitError),
        ("DataLoss", ProviderError),
    ],
)
def test_wrap_error_classifies_error_code_subclasses_by_their_status(
    error_name: str, expected: type
):
    # The SDK raises the error_code subclass (ResourceDoesNotExist) in preference to the
    # status class (NotFound); an exact-type lookup misses it and returns the raw error.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors import platform

    wrapped = _connection_config().wrap_error(getattr(platform, error_name)(SECRET))

    assert type(wrapped) is expected
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_classifies_aborted_as_a_provider_error():
    # ABORTED is a 409 and would otherwise land with the rest of the 4xx as a UserError,
    # which the platform treats as terminal. It is a Databricks-side concurrency
    # conflict -- not the customer's doing, and usually cleared by retrying -- so it is
    # classified as a provider failure, which stays retryable.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import Aborted

    wrapped = _connection_config().wrap_error(Aborted(SECRET))

    assert type(wrapped) is ProviderError
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_unhandled_log_redacts(caplog: pytest.LogCaptureFixture):
    # A non-Databricks, non-auth ValueError falls through to the unhandled
    # log path and is returned raw; the log line must still be redacted.
    pytest.importorskip("databricks.sdk")
    with caplog.at_level(logging.ERROR, logger="unstructured_ingest"):
        _connection_config().wrap_error(RuntimeError(SECRET))

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


def _indexer(
    mocker: MockerFixture, client, volume_path: str = "path"
) -> DatabricksNativeVolumesIndexer:
    mocker.patch.object(DatabricksNativeVolumesConnectionConfig, "get_client", return_value=client)
    return DatabricksNativeVolumesIndexer(
        connection_config=_connection_config(),
        index_config=DatabricksNativeVolumesIndexerConfig(
            catalog="catalog", schema="schema", volume="volume", volume_path=volume_path
        ),
    )


def test_indexer_precheck_raises_when_credentials_are_rejected(mocker: MockerFixture):
    # Constructing the client makes no request under token auth, so without a live
    # call the connection check passes for a token the workspace rejects and the
    # failure only surfaces when the job runs.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = mocker.MagicMock()
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[401](SECRET)

    with pytest.raises(UserAuthError) as exc_info:
        _indexer(mocker, client).precheck()

    assert SECRET not in str(exc_info.value)
    client.current_user.me.assert_called_once()


def test_indexer_precheck_raises_when_volume_path_is_missing(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = mocker.MagicMock()
    client.dbfs.list.side_effect = STATUS_CODE_MAPPING[404]("path does not exist")

    with pytest.raises(UserError):
        _indexer(mocker, client).precheck()


def test_indexer_precheck_raises_when_volume_read_is_not_granted(mocker: MockerFixture):
    # Credentials are good but the Unity Catalog READ VOLUME grant is missing: the
    # me() call succeeds and only the listing fails.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = mocker.MagicMock()
    client.dbfs.list.side_effect = STATUS_CODE_MAPPING[403]("insufficient permissions")

    with pytest.raises(UserAuthError):
        _indexer(mocker, client).precheck()


def test_indexer_precheck_lists_the_configured_path_without_recursing(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    client = mocker.MagicMock()
    client.dbfs.list.return_value = iter(
        [mocker.MagicMock(is_dir=False, path="/Volumes/catalog/schema/volume/path/example.pdf")]
    )

    _indexer(mocker, client).precheck()

    client.dbfs.list.assert_called_once_with(
        path="/Volumes/catalog/schema/volume/path", recursive=False
    )


def test_indexer_precheck_accepts_an_empty_volume_path(mocker: MockerFixture):
    # With no volume_path the source is the volume root, and an empty listing there
    # is a valid (empty) source, not a connection failure.
    pytest.importorskip("databricks.sdk")
    client = mocker.MagicMock()
    client.dbfs.list.return_value = iter([])

    _indexer(mocker, client, volume_path="").precheck()

    # precheck validates BOTH credentials (me()) and resource access (list()); assert both,
    # since the empty-path case must still prove the connection was actually contacted.
    client.current_user.me.assert_called_once()
    client.dbfs.list.assert_called_once_with(path="/Volumes/catalog/schema/volume", recursive=False)


def test_indexer_precheck_error_does_not_leak_raw_text_in_traceback(mocker: MockerFixture):
    # wrap_error sanitizes the message, but if the raw SDK exception survives as the
    # implicit __context__ then a full traceback (logger.exception / format_exception)
    # reprints its secret-bearing text. The precheck must suppress the context.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = mocker.MagicMock()
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[401](SECRET)

    with pytest.raises(UserAuthError) as exc_info:
        _indexer(mocker, client).precheck()

    formatted = "".join(traceback.format_exception(exc_info.value))
    assert SECRET not in formatted
    assert "hunter2" not in formatted


def test_indexed_file_reports_modification_time_in_epoch_seconds(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    indexer = DatabricksNativeVolumesIndexer(
        connection_config=_connection_config(),
        index_config=DatabricksNativeVolumesIndexerConfig(
            catalog="catalog", volume="volume", volume_path="path"
        ),
    )
    file_info = mocker.MagicMock(
        is_dir=False, path="/Volumes/catalog/schema/volume/path/example.pdf"
    )
    # The Databricks SDK reports modification_time in milliseconds.
    file_info.modification_time = 1729186569000
    client = mocker.MagicMock()
    client.dbfs.list.return_value = [file_info]
    mocker.patch.object(_connection_config().__class__, "get_client", return_value=client)

    file_data = next(iter(indexer.run()))

    assert parse_timestamp(file_data.metadata.date_modified) == 1729186569.0


def _uploader(
    mocker: MockerFixture, client, volume_path: str = "path"
) -> DatabricksNativeVolumesUploader:
    mocker.patch.object(DatabricksNativeVolumesConnectionConfig, "get_client", return_value=client)
    return DatabricksNativeVolumesUploader(
        connection_config=_connection_config(),
        upload_config=DatabricksNativeVolumesUploaderConfig(
            catalog="catalog", schema="schema", volume="volume", volume_path=volume_path
        ),
    )


PRINCIPAL = "someone@example.com"


def _effective_permissions(*privilege_names: str):
    """A real EffectivePermissionsList, as grants.get_effective returns it."""
    from databricks.sdk.service.catalog import (
        EffectivePermissionsList,
        EffectivePrivilege,
        EffectivePrivilegeAssignment,
        Privilege,
    )

    return EffectivePermissionsList(
        privilege_assignments=[
            EffectivePrivilegeAssignment(
                principal=PRINCIPAL,
                privileges=[
                    EffectivePrivilege(privilege=Privilege(name)) for name in privilege_names
                ],
            )
        ]
    )


def _uploader_client(mocker: MockerFixture, *privilege_names: str):
    """A client whose Unity Catalog reads all answer: credentials, volume, grants, path."""
    client = mocker.MagicMock()
    client.current_user.me.return_value.user_name = PRINCIPAL
    client.grants.get_effective.return_value = _effective_permissions(
        *(privilege_names or ("WRITE_VOLUME",))
    )
    return client


def test_uploader_precheck_raises_when_credentials_are_rejected(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[401](SECRET)

    with pytest.raises(UserAuthError) as exc_info:
        _uploader(mocker, client).precheck()

    assert SECRET not in str(exc_info.value)
    client.volumes.read.assert_not_called()


def test_uploader_precheck_error_does_not_leak_raw_text_in_traceback(mocker: MockerFixture):
    # Same trap as the indexer: wrap_error sanitizes the message, but the raw SDK
    # exception surviving as __context__ reprints its secret-bearing text in any
    # full traceback.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[401](SECRET)

    with pytest.raises(UserAuthError) as exc_info:
        _uploader(mocker, client).precheck()

    formatted = "".join(traceback.format_exception(exc_info.value))
    assert SECRET not in formatted
    assert "hunter2" not in formatted


@pytest.mark.parametrize("status_code", [429, 500, 503])
def test_uploader_precheck_passes_when_the_identity_call_is_not_an_access_answer(
    mocker: MockerFixture, status_code: int
):
    # A throttle or a Databricks-side outage on me() says nothing about the credentials,
    # so it must not abort the check the way a 401 does.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[status_code]("try again later")

    _uploader(mocker, client).precheck()


def test_uploader_precheck_passes_on_an_unrecognised_identity_failure(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # A socket error is not an SDK status at all, so it has no place in the fatal set.
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker)
    client.current_user.me.side_effect = RuntimeError(SECRET)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


def test_uploader_precheck_reads_unity_catalog_and_writes_nothing(mocker: MockerFixture):
    # The destination is a volume something else is usually watching: an Auto Loader
    # stream or a file-arrival trigger cannot tell a probe file from real input. The
    # check has to answer with reads only.
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker)

    _uploader(mocker, client).precheck()

    client.current_user.me.assert_called_once()
    client.volumes.read.assert_called_once_with("catalog.schema.volume")
    client.grants.get_effective.assert_called_once_with(
        "VOLUME", "catalog.schema.volume", principal=PRINCIPAL
    )
    client.files.get_directory_metadata.assert_called_once_with(
        directory_path="/Volumes/catalog/schema/volume/path"
    )
    client.files.upload.assert_not_called()
    client.files.delete.assert_not_called()


def test_uploader_precheck_checks_the_volume_root_when_no_volume_path_is_set(
    mocker: MockerFixture,
):
    # With no volume_path the destination IS the volume root, which volumes.read has
    # already covered, so there is no directory left to look up.
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker)

    _uploader(mocker, client, volume_path="").precheck()

    client.volumes.read.assert_called_once_with("catalog.schema.volume")
    client.files.get_directory_metadata.assert_not_called()


def test_uploader_precheck_raises_when_the_volume_does_not_exist(mocker: MockerFixture):
    # The 404 this check exists to catch. Unity Catalog answers the grant lookup with the
    # same 404, so nothing contradicts it.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.grants.get_effective.side_effect = STATUS_CODE_MAPPING[404]("no such volume")
    client.volumes.read.side_effect = STATUS_CODE_MAPPING[404]("no such volume")

    with pytest.raises(UserError) as exc_info:
        _uploader(mocker, client).precheck()

    # The volume and the path are the facts the old check hid; the failure names both.
    assert "catalog.schema.volume" in str(exc_info.value)
    assert "/Volumes/catalog/schema/volume/path" in str(exc_info.value)


def test_uploader_precheck_raises_when_a_404_subclass_says_the_volume_is_missing(
    mocker: MockerFixture,
):
    # The SDK prefers the error_code class over the status class, so a missing volume can
    # arrive as ResourceDoesNotExist (a NotFound subclass) rather than NotFound itself.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import ResourceDoesNotExist

    client = _uploader_client(mocker)
    client.grants.get_effective.side_effect = ResourceDoesNotExist(SECRET)
    client.volumes.read.side_effect = ResourceDoesNotExist(SECRET)

    with pytest.raises(UserError) as exc_info:
        _uploader(mocker, client).precheck()

    assert "catalog.schema.volume" in str(exc_info.value)
    formatted = "".join(traceback.format_exception(exc_info.value))
    assert SECRET not in formatted
    assert "hunter2" not in formatted


def test_uploader_precheck_passes_when_the_grant_lookup_contradicts_a_404(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # A grant lookup that resolved the securable proves the volume is there, so a 404
    # from the metadata read is about what this principal may see, not about existence.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.volumes.read.side_effect = STATUS_CODE_MAPPING[404]("not visible to you")

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert "catalog.schema.volume" in caplog.text


def test_uploader_precheck_passes_when_the_volume_read_is_forbidden(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # WRITE VOLUME does not imply READ VOLUME, so a principal that can write here can
    # still be refused the metadata read. Failing on that 403 refuses a destination that
    # works.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.volumes.read.side_effect = STATUS_CODE_MAPPING[403](SECRET)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


@pytest.mark.parametrize("status_code", [429, 500, 503])
def test_uploader_precheck_passes_when_the_volume_read_is_not_an_access_answer(
    mocker: MockerFixture, status_code: int
):
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.volumes.read.side_effect = STATUS_CODE_MAPPING[status_code]("try again later")

    _uploader(mocker, client).precheck()


def test_uploader_precheck_passes_on_an_unrecognised_volume_read_failure(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker)
    client.volumes.read.side_effect = RuntimeError(SECRET)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


def test_uploader_precheck_warns_when_write_volume_is_not_granted(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # Never a refusal: the effective-permissions API is documented to expand parent
    # securables, not group membership, and a grant held through a group is the normal
    # case. So an absent WRITE_VOLUME is reported, not acted on.
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker, "READ_VOLUME")

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert "WRITE VOLUME" in caplog.text
    assert "catalog.schema.volume" in caplog.text


@pytest.mark.parametrize("privilege_name", ["WRITE_VOLUME", "ALL_PRIVILEGES"])
def test_uploader_precheck_is_quiet_when_write_volume_is_granted(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture, privilege_name: str
):
    pytest.importorskip("databricks.sdk")
    client = _uploader_client(mocker, privilege_name)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert caplog.text == ""


def test_uploader_precheck_passes_when_the_grant_lookup_is_refused(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # A principal that does not own the volume may not be allowed to read its own
    # effective grants, so a 403 here is not an answer about writing.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.grants.get_effective.side_effect = STATUS_CODE_MAPPING[403](SECRET)

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert "WRITE VOLUME is unverified" in caplog.text
    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


def test_uploader_precheck_warns_when_the_volume_path_is_missing(
    mocker: MockerFixture, caplog: pytest.LogCaptureFixture
):
    # Not a refusal: the uploader never creates a directory, so the Files API is already
    # relied on to make the parents at upload time.
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = _uploader_client(mocker)
    client.files.get_directory_metadata.side_effect = STATUS_CODE_MAPPING[404]("no such path")

    with caplog.at_level(logging.WARNING, logger="unstructured_ingest"):
        _uploader(mocker, client).precheck()

    assert "/Volumes/catalog/schema/volume/path" in caplog.text
