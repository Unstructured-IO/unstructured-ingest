import logging

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.error import ProviderError, UserAuthError, UserError
from unstructured_ingest.processes.connectors.databricks.volumes import (
    DatabricksVolumesDownloaderConfig,
)
from unstructured_ingest.processes.connectors.databricks.volumes_native import (
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
    DatabricksNativeVolumesDownloader,
    DatabricksNativeVolumesIndexer,
    DatabricksNativeVolumesIndexerConfig,
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


def test_wrap_error_unhandled_log_redacts(caplog: pytest.LogCaptureFixture):
    # A non-Databricks, non-auth ValueError falls through to the unhandled
    # log path and is returned raw; the log line must still be redacted.
    pytest.importorskip("databricks.sdk")
    with caplog.at_level(logging.ERROR, logger="unstructured_ingest"):
        _connection_config().wrap_error(RuntimeError(SECRET))

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text


def _indexer(mocker: MockerFixture, client) -> DatabricksNativeVolumesIndexer:
    mocker.patch.object(DatabricksNativeVolumesConnectionConfig, "get_client", return_value=client)
    return DatabricksNativeVolumesIndexer(
        connection_config=_connection_config(),
        index_config=DatabricksNativeVolumesIndexerConfig(
            catalog="catalog", schema="schema", volume="volume", volume_path="path"
        ),
    )


def _downloader(mocker: MockerFixture, client) -> DatabricksNativeVolumesDownloader:
    mocker.patch.object(DatabricksNativeVolumesConnectionConfig, "get_client", return_value=client)
    return DatabricksNativeVolumesDownloader(
        connection_config=_connection_config(),
        download_config=DatabricksVolumesDownloaderConfig(),
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
    # An empty directory is a valid source, not a connection failure.
    pytest.importorskip("databricks.sdk")
    client = mocker.MagicMock()
    client.dbfs.list.return_value = iter([])

    _indexer(mocker, client).precheck()


def test_downloader_precheck_raises_when_credentials_are_rejected(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    client = mocker.MagicMock()
    client.current_user.me.side_effect = STATUS_CODE_MAPPING[401](SECRET)

    with pytest.raises(UserAuthError) as exc_info:
        _downloader(mocker, client).precheck()

    assert SECRET not in str(exc_info.value)
    client.current_user.me.assert_called_once()


def test_downloader_precheck_passes_when_credentials_are_accepted(mocker: MockerFixture):
    pytest.importorskip("databricks.sdk")
    client = mocker.MagicMock()

    _downloader(mocker, client).precheck()

    client.current_user.me.assert_called_once()


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
