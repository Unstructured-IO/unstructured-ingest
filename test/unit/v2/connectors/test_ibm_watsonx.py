import time
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import Secret
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo
from pytest_mock import MockerFixture

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.ibm_watsonx import (
    CONNECTOR_TYPE as IBM_WATSONX_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.ibm_watsonx import (
    IbmWatsonxAccessConfig,
    IbmWatsonxConnectionConfig,
    IbmWatsonxUploader,
    IbmWatsonxUploaderConfig,
    _transaction_wrapper,
)


@pytest.fixture
def file_data():
    return FileData(
        identifier="test_identifier",
        connector_type=IBM_WATSONX_CONNECTOR_TYPE,
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
        max_retries=3,
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
    mock_get_table = mocker.patch.context_manager(
        IbmWatsonxUploader, "get_table", autospec=True
    )
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
def mock_transaction_wrapper(mocker: MockerFixture, mock_transaction: MagicMock):
    return mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx._transaction_wrapper",
        side_effect=lambda table, fn, max_retries, **kwargs: fn(mock_transaction, **kwargs),
    )


@pytest.fixture
def timestamp_now():
    return int(time.time())


def test_ibm_watsonx_connection_config_iceberg_url(
    mocker: MockerFixture,
    connection_config: IbmWatsonxConnectionConfig,
):
    mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.DEFAULT_ICEBERG_URI_PATH",
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
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.DEFAULT_ICEBERG_URI_PATH",
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
        "test_catalog",
        **{
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
    with pytest.raises(DestinationConnectionError):
        with connection_config.get_catalog():
            pass


def test_precheck_namespace_exists_table_exists(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_called_once_with(("test_namespace", "test_table"))


def test_precheck_namespace_does_not_exist(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    mock_catalog.namespace_exists.return_value = False

    with pytest.raises(
        DestinationConnectionError, match="Namespace 'test_namespace' does not exist"
    ):
        uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_not_called()


def test_precheck_table_does_not_exist(
    mock_get_catalog: MagicMock,
    mock_catalog: MagicMock,
    uploader: IbmWatsonxUploader,
):
    mock_catalog.table_exists.return_value = False

    with pytest.raises(
        DestinationConnectionError,
        match="Table 'test_table' does not exist in namespace 'test_namespace'",
    ):
        uploader.precheck()

    mock_catalog.namespace_exists.assert_called_once_with("test_namespace")
    mock_catalog.table_exists.assert_called_once_with(("test_namespace", "test_table"))


def test_transaction_wrapper_success(
    mocker: MockerFixture, mock_table: MagicMock, mock_transaction: MagicMock
):
    mock_fn = mocker.MagicMock()

    _transaction_wrapper(
        mock_table, mock_fn, max_retries=3, test_arg_1="test_arg_1", test_arg_2="test_arg_2"
    )

    mock_table.transaction.assert_called_once()
    mock_fn.assert_called_once_with(
        mock_transaction, test_arg_1="test_arg_1", test_arg_2="test_arg_2"
    )
    mock_table.refresh.assert_not_called()


def test_transaction_wrapper_succeed_after_refresh(
    mocker: MockerFixture, mock_table: MagicMock, mock_transaction: MagicMock
):
    mock_fn = mocker.MagicMock()
    mock_fn.side_effect = CommitFailedException

    def remove_side_effect():
        mock_fn.side_effect = None

    mock_table.refresh.side_effect = remove_side_effect

    _transaction_wrapper(mock_table, mock_fn, max_retries=3)

    assert mock_table.transaction.call_count == 2
    assert mock_fn.call_count == 2
    assert mock_table.refresh.call_count == 1


def test_transaction_wrapper_other_exception(
    mocker: MockerFixture, mock_table: MagicMock, mock_transaction: MagicMock
):
    mock_fn = mocker.MagicMock()
    mock_fn.side_effect = Exception("Test exception")

    with pytest.raises(
        DestinationConnectionError, match="Failed to append data to table: Test exception"
    ):
        _transaction_wrapper(mock_table, mock_fn, max_retries=3)

    mock_table.transaction.assert_called_once()
    mock_fn.assert_called_once_with(mock_transaction)
    mock_table.refresh.assert_not_called()


def test_transaction_wrapper_max_retries_exceeded(
    mocker: MockerFixture, mock_table: MagicMock, mock_transaction: MagicMock
):

    mock_fn = mocker.MagicMock()
    mock_fn.side_effect = CommitFailedException

    with pytest.raises(
        DestinationConnectionError, match="Failed to commit transaction after 3 retries"
    ):
        _transaction_wrapper(mock_table, mock_fn, max_retries=3)

    assert mock_table.transaction.call_count == 3
    assert mock_fn.call_count == 3
    assert mock_table.refresh.call_count == 3


def test_upload_data_success(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_get_table: MagicMock,
    mock_table: MagicMock,
    mock_transaction: MagicMock,
    mock_data_table: MagicMock,
    mock_transaction_wrapper: MagicMock,
    file_data: FileData,
):
    mock_column_names = mocker.MagicMock()
    mock_column_names.column_names = ["test_column_1", "test_record_id_key", "test_column_2"]
    mock_transaction._table.schema.return_value = mock_column_names

    uploader.upload_data(mock_table, mock_data_table, file_data)

    mock_transaction_wrapper.assert_called_once_with(
        table=mock_table,
        fn=mocker.ANY,
        max_retries=uploader.upload_config.max_retries,
        file_data=file_data,
        data_table=mock_data_table,
    )
    mock_transaction.delete.assert_called_once_with(
        delete_filter=EqualTo("test_record_id_key", "test_identifier")
    )
    mock_transaction.append.assert_called_once_with(mock_data_table)


def test_upload_data_success_no_delete(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_get_table: MagicMock,
    mock_table: MagicMock,
    mock_transaction: MagicMock,
    mock_data_table: MagicMock,
    mock_transaction_wrapper: MagicMock,
    file_data: FileData,
):
    mock_column_names = mocker.MagicMock()
    mock_column_names.column_names = ["test_column_1", "test_column_2"]
    mock_transaction._table.schema.return_value = mock_column_names

    uploader.upload_data(mock_table, mock_data_table, file_data)

    mock_transaction_wrapper.assert_called_once_with(
        table=mock_table,
        fn=mocker.ANY,
        max_retries=uploader.upload_config.max_retries,
        file_data=file_data,
        data_table=mock_data_table,
    )
    mock_transaction.delete.assert_not_called()
    mock_transaction.append.assert_called_once_with(mock_data_table)


def test_get_data_table_success(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_table: MagicMock,
):
    mock_df = pd.DataFrame(
        {
            "test_column_0": [True, False, True],
            "test_column_1": [1, 2, 3],
            "test_column_2": ["a", "b", "c"],
        }
    )
    mock_table.schema.return_value.column_names = [
        "test_column_1",
        "test_column_2",
        "test_column_3",
    ]
    mock_get_data_df = mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.get_data_df",
        return_value=mock_df,
    )

    path = Path("/tmp/test_file.pdf")
    result = uploader._get_data_table(mock_table, path)

    mock_get_data_df.assert_called_once_with(path)
    assert len(result.column_names) == 2
    assert "test_column_1" in result.column_names
    assert "test_column_2" in result.column_names


def test_get_data_table_no_common_columns(
    mocker: MockerFixture,
    uploader: IbmWatsonxUploader,
    mock_table: MagicMock,
):
    mock_df = pd.DataFrame({"test_column_4": [1, 2, 3], "test_column_5": ["a", "b", "c"]})
    mock_table.schema.return_value.column_names = [
        "test_column_1",
        "test_column_2",
        "test_column_3",
    ]
    mock_get_data_df = mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx.get_data_df",
        return_value=mock_df,
    )

    path = Path("/tmp/test_file.pdf")
    with pytest.raises(ValueError, match="Iceberg table schema doesn't contain proper columns"):
        uploader._get_data_table(mock_table, path)
    mock_get_data_df.assert_called_once_with(path)
