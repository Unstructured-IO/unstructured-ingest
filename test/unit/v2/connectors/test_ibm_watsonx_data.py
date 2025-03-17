import time

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.v2.processes.connectors.ibm_watsonx_data import (
    IbmWatsonxDataAccessConfig,
    IbmWatsonxDataConnectionConfig,
)


@pytest.fixture
def access_config():
    return IbmWatsonxDataAccessConfig(
        iam_api_key="test_iam_api_key",
        access_key_id="test_access_key_id",
        secret_access_key="test_secret_access_key",
    )


@pytest.fixture
def connection_config(access_config: IbmWatsonxDataAccessConfig):
    return IbmWatsonxDataConnectionConfig(
        access_config=Secret(access_config),
        iceberg_endpoint="test_iceberg_endpoint/",
        object_storage_endpoint="test_object_storage_endpoint/",
        object_storage_region="test_region",
        catalog="test_catalog",
    )


@pytest.fixture
def timestamp_now():
    return int(time.time())


def test_ibm_watsonx_data_connection_config_iceberg_url(
    mocker: MockerFixture,
    connection_config: IbmWatsonxDataConnectionConfig,
):
    mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx_data.DEFAULT_ICEBERG_URI_PATH",
        new="/mds/iceberg",
    )
    expected_url = "https://test_iceberg_endpoint/mds/iceberg"
    assert connection_config.iceberg_url == expected_url


def test_ibm_watsonx_data_connection_config_object_storage_url(
    connection_config: IbmWatsonxDataConnectionConfig,
):
    expected_url = "https://test_object_storage_endpoint"
    assert connection_config.object_storage_url == expected_url


def test_ibm_watsonx_data_connection_config_bearer_token_new_token(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig, timestamp_now: int
):
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxDataConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_data_connection_config_bearer_token_existing_token(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "existing_token",
        "expiration": timestamp_now + 3600,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxDataConnectionConfig, "generate_bearer_token"
    )
    token = connection_config.bearer_token
    assert token == "existing_token"
    mock_generate_bearer_token.assert_not_called()


def test_ibm_watsonx_data_connection_config_bearer_token_expired_token(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "expired_token",
        "expiration": timestamp_now - 3600,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxDataConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_data_connection_config_bearer_token_soon_to_expire_token(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig, timestamp_now: int
):
    connection_config._bearer_token = {
        "access_token": "soon_to_expire_token",
        "expiration": timestamp_now + 60,
    }
    mock_generate_bearer_token = mocker.patch.object(
        IbmWatsonxDataConnectionConfig,
        "generate_bearer_token",
        return_value={"access_token": "new_token", "expiration": timestamp_now + 3600},
    )
    token = connection_config.bearer_token
    assert token == "new_token"
    mock_generate_bearer_token.assert_called_once()


def test_ibm_watsonx_data_connection_config_get_catalog_success(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig
):
    mocker.patch(
        "unstructured_ingest.v2.processes.connectors.ibm_watsonx_data.DEFAULT_ICEBERG_URI_PATH",
        new="/mds/iceberg",
    )
    mocker.patch.object(
        IbmWatsonxDataConnectionConfig,
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


def test_ibm_watsonx_data_connection_config_get_catalog_failure(
    mocker: MockerFixture, connection_config: IbmWatsonxDataConnectionConfig
):
    mocker.patch(
        "pyiceberg.catalog.load_catalog",
        side_effect=Exception("Connection error"),
    )
    with pytest.raises(DestinationConnectionError):
        with connection_config.get_catalog():
            pass
