import time
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import Secret
from pyiceberg.exceptions import CommitFailedException
from pytest_mock import MockerFixture

from unstructured_ingest.v2.errors import ProviderError, UserError
from unstructured_ingest.v2.processes.connectors.ibm_watsonx import IBM_WATSONX_S3_CONNECTOR_TYPE
from unstructured_ingest.v2.processes.connectors.ibm_watsonx.ibm_watsonx_s3 import (
    IbmWatsonxAccessConfig,
    IbmWatsonxConnectionConfig,
    IbmWatsonxUploader,
    IbmWatsonxUploaderConfig,
)
from unstructured_ingest.v2.types.file_data import FileData, SourceIdentifiers


@pytest.fixture
def file_data():
    return FileData(
        identifier="test_identifier",
        connector_type=IBM_WATSONX_S3_CONNECTOR_TYPE,
        source_identifiers=SourceIdentifiers(
            filename="test_file.pdf", fullpath="/tmp/test_file.pdf"
        ),
    )


@pytest.fixture
def access_config():
    return IbmWatsonxAccessConfig(
        iam_api_key="test_iam_api_key",
        access_key_id="test_access_key_id",
        secret_access_key="test_secret_access_key",
    )


@pytest.fixture
def connection_config(access_config: IbmWatsonxAccessConfig):
    return IbmWatsonxConnectionConfig(
        access_config=Secret(access_config),
        iceberg_endpoint="test_iceberg_endpoint/",
        object_storage_endpoint="test_object_storage_endpoint/",
        object_storage_region="test_region",
        catalog="test_catalog",
    )


@pytest.fixture
def uploader_config():
    return IbmWatsonxUploaderConfig(
        namespace="test_namespace",
        table="test_table",
        record_id_key="test_record_id_key",
    )


@pytest.fixture
def uploader(
    connection_config: IbmWatsonxConnectionConfig, uploader_config: IbmWatsonxUploaderConfig
):
    return IbmWatsonxUploader(
        connection_config=connection_config,
        upload_config=uploader_config,
    )


@pytest.fixture
def mock_catalog(mocker: MockerFixture):
    mock_catalog = mocker.MagicMock()
    mock_catalog.namespace_exists.return_value = True
    mock_catalog.table_exists.return_value = True
    return mock_catalog


@pytest.fixture
def mock_get_catalog(mocker: MockerFixture, mock_catalog: MagicMock):
    mock_get_catalog = mocker.patch.context_manager(
        IbmWatsonxConnectionConfig, "get_catalog", autospec=True
    )
    mock_get_catalog.return_value.__enter__.return_value = mock_catalog
    return mock_get_catalog


@pytest.fixture
def mock_table(mocker: MockerFixture):
    mock_table = mocker.MagicMock()
    return mock_table


@pytest.fixture
def mock_get_table(mocker: MockerFixture, mock_table: MagicMock):
    mock_get_table = mocker.patch.context_manager(IbmWatsonxUploader, "get_table", autospec=True)
    mock_get_table.return_value.__enter__.return_value = mock_table
    return mock_get_table


@pytest.fixture
def mock_transaction(mocker: MockerFixture, mock_table: MagicMock):
    mock_transaction = mocker.MagicMock()
    mock_table.transaction.return_value.__enter__.return_value = mock_transaction
    return mock_transaction


@pytest.fixture
def mock_data_table(mocker: MockerFixture):
    mock_data_table = mocker.MagicMock()
    mock_data_table.schema = "schema"
    return mock_data_table


@pytest.fixture
def mock_delete(mocker: MockerFixture):
    return mocker.patch.object(IbmWatsonxUploader, "_delete")


@pytest.fixture
def test_df():
    return pd.DataFrame(
        {
            "test_column_0": [True, False, True],
            "test_column_1": [1, 2, 3],
            "test_column_2": ["a", "b", "c"],
        }
    )


@pytest.fixture
def timestamp_now():
    return int(time.time())


def test_ibm_watsonx_connection_config_iceberg_url(
    mocker: MockerFixture,
    connection_config: IbmWatsonxConnectionConfig,
):
    mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.ibm_watsonx_s3.DEFAULT_ICEBERG_URI_PATH",  # noqa: E501
        new="/mds/iceberg",
    )
    expected_url = "https://test_iceberg_endpoint/mds/iceberg"
    assert connection_config.iceberg_url == expected_url


def test_ibm_watsonx_connection_config_object_storage_url(
    connection_config: IbmWatsonxConnectionConfig,
):
    expected_url = "https://test_object_storage_endpoint"
    assert connection_config.object_storage_url == expected_url


def test_ibm_watsonx_connection_config_bearer_token_new_token(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig, timestamp_now: int
):
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_connection_config_bearer_token_existing_token(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "existing_token",
        "expiration": timestamp_now + 3600,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxConnectionConfig, "generate_bearer_token"
    )
    token = connection_config.bearer_token
    assert token == "existing_token"
    mock_generate_bearer_token.assert_not_called()


def test_ibm_watsonx_connection_config_bearer_token_expired_token(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "expired_token",
        "expiration": timestamp_now - 3600,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_connection_config_bearer_token_soon_to_expire_token(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "soon_to_expire_token",
        "expiration": timestamp_now + 60,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_connection_config_get_catalog_success(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig
):
    mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.ibm_watsonx_s3.DEFAULT_ICEBERG_URI_PATH",  # noqa: E501
        new="/mds/iceberg",
    )
    mocker.patch.object(
        IbmWatsonxConnectionConfig,
        "bearer_token",
        new="test_bearer_token",
    )
    mock_load_catalog = mocker.patch("pyiceberg.catalog.load_catalog")

    with connection_config.get_catalog() as catalog:
        assert catalog is not None
    mock_load_catalog.assert_called_once_with(
        **{
            "name": "test_catalog",
            "type": "rest",
            "uri": "https://test_iceberg_endpoint/mds/iceberg",
            "token": "test_bearer_token",
            "warehouse": "test_catalog",
            "s3.endpoint": "https://test_object_storage_endpoint",
            "s3.access-key-id": "test_access_key_id",
            "s3.secret-access-key": "test_secret_access_key",
            "s3.region": "test_region",
        }
    )


def test_ibm_watsonx_connection_config_get_catalog_failure(
    mocker: MockerFixture, connection_config: IbmWatsonxConnectionConfig
):
    mocker.patch(
        "pyiceberg.catalog.load_catalog",
        side_effect=Exception("Connection error"),
    )
    mocker.patch.object(
        IbmWatsonxConnectionConfig,
        "bearer_token",
        new="test_bearer_token",
    )
    with pytest.raises(ProviderError):
        with connection_config.get_catalog():
            pass


def test_ibm_watsonx_uploader_precheck_namespace_exists_table_exists(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_called_once_with(("test_namespace", "test_table"))


def test_ibm_watsonx_uploader_precheck_namespace_does_not_exist(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    mock_catalog.namespace_exists.return_value = False

    with pytest.raises(UserError, match="Namespace 'test_namespace' does not exist"):
        uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_not_called()


def test_ibm_watsonx_uploader_precheck_table_does_not_exist(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    mock_catalog.table_exists.return_value = False

    with pytest.raises(
        UserError,
        match="Table 'test_table' does not exist in namespace 'test_namespace'",
    ):
        uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_called_once_with(("test_namespace", "test_table"))


def test_ibm_watsonx_uploader_upload_data_table_success(
    uploader: IbmWatsonxUploader,
    mock_table: MagicMock,
    mock_transaction: MagicMock,
    mock_data_table: MagicMock,
    mock_delete: MagicMock,
    file_data: FileData,
):
    uploader.upload_data_table(mock_table, mock_data_table, file_data)

    mock_delete.assert_called_once_with(mock_transaction, "test_identifier")
    mock_transaction.append.assert_called_once_with(mock_data_table)


def test_ibm_watsonx_uploader_upload_data_table_commit_exception(
    uploader: IbmWatsonxUploader,
    mock_table: MagicMock,
    mock_transaction: MagicMock,
    mock_data_table: MagicMock,
    mock_delete: MagicMock,
    file_data: FileData,
):
    mock_transaction.append.side_effect = CommitFailedException()

    with pytest.raises(ProviderError):
        uploader.upload_data_table(mock_table, mock_data_table, file_data)
    assert mock_table.refresh.call_count == 5


def test_ibm_watsonx_uploader_upload_data_table_exception(
    uploader: IbmWatsonxUploader,
    mock_table: MagicMock,
    mock_transaction: MagicMock,
    mock_data_table: MagicMock,
    mock_delete: MagicMock,
    file_data: FileData,
):
    mock_transaction.append.side_effect = Exception()

    with pytest.raises(ProviderError):
        uploader.upload_data_table(mock_table, mock_data_table, file_data)
    assert mock_table.refresh.call_count == 0


def test_ibm_watsonx_uploader_df_to_arrow_table(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    test_df: pd.DataFrame,
):
    mock_fit_to_schema = mocker.patch.object(
        IbmWatsonxUploader, "_fit_to_schema", return_value=test_df
    )

    result = uploader._df_to_arrow_table(test_df)

    mock_fit_to_schema.assert_called_once_with(test_df, add_missing_columns=False)
    assert len(result.column_names) == 3
    assert "test_column_0" in result.column_names
    assert "test_column_1" in result.column_names
    assert "test_column_2" in result.column_names


def test_ibm_watsonx_uploader_can_delete_column_exists(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
):
    mocker.patch.object(
        IbmWatsonxUploader, "get_table_columns", return_value=["test_record_id_key"]
    )

    assert uploader.can_delete() is True


def test_ibm_watsonx_uploader_can_delete_column_does_not_exist(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
):
    mocker.patch.object(IbmWatsonxUploader, "get_table_columns", return_value=["other_column"])

    assert uploader.can_delete() is False


def test_ibm_watsonx_uploader_get_table_columns_cache(
    uploader: IbmWatsonxUploader,
):
    uploader._columns = ["cached_column"]

    result = uploader.get_table_columns()

    assert result == ["cached_column"]


def test_ibm_watsonx_uploader_get_table_columns_no_cache(
    uploader: IbmWatsonxUploader,
    mock_get_table: MagicMock,
    mock_table: MagicMock,
):
    uploader._columns = None
    mock_table.schema.return_value.column_names = ["column_1", "column_2"]

    result = uploader.get_table_columns()

    mock_get_table.assert_called_once()
    assert result == ["column_1", "column_2"]
    assert uploader._columns == ["column_1", "column_2"]


def test_ibm_watsonx_uploader_upload_dataframe_success(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    test_df: pd.DataFrame,
    mock_get_table: MagicMock,
    mock_table: MagicMock,
    mock_data_table: MagicMock,
    file_data: FileData,
):
    mocker.patch.object(IbmWatsonxUploader, "_df_to_arrow_table", return_value=mock_data_table)
    mock_upload_data_table = mocker.patch.object(IbmWatsonxUploader, "upload_data_table")

    uploader.upload_dataframe(test_df, file_data)

    mock_get_table.assert_called_once()
    mock_upload_data_table.assert_called_once_with(mock_table, mock_data_table, file_data)


def test_ibm_watsonx_uploader_delete_can_delete(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_transaction: MagicMock,
):
    mocker.patch.object(IbmWatsonxUploader, "can_delete", return_value=True)
    mock_equal_to = mocker.patch("pyiceberg.expressions.EqualTo")

    uploader._delete(mock_transaction, "test_identifier")

    mock_equal_to.assert_called_once_with("test_record_id_key", "test_identifier")
    mock_transaction.delete.assert_called_once_with(delete_filter=mock_equal_to.return_value)


def test_ibm_watsonx_uploader_delete_cannot_delete(
    caplog: pytest.LogCaptureFixture,
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_transaction: MagicMock,
):
    mocker.patch.object(IbmWatsonxUploader, "can_delete", return_value=False)

    uploader._delete(mock_transaction, "test_identifier")
    mock_transaction.delete.assert_not_called()
    assert (
        "Table doesn't contain expected record id column test_record_id_key, skipping delete"
        in caplog.text
    )
