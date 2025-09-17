from unittest.mock import MagicMock

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.processes.connectors.sql.snowflake import (
    SnowflakeAccessConfig,
    SnowflakeConnectionConfig,
    SnowflakeUploader,
)


@pytest.fixture
def snowflake_access_config():
    return SnowflakeAccessConfig(
        password="password",
    )


@pytest.fixture
def snowflake_connection_config(snowflake_access_config: SnowflakeAccessConfig):
    return SnowflakeConnectionConfig(
        account="account",
        user="user",
        host="host",
        port=1234,
        database="database",
        schema="schema",
        role="role",
        access_config=Secret(snowflake_access_config),
    )


@pytest.fixture
def snowflake_uploader(snowflake_connection_config: SnowflakeConnectionConfig):
    uploader = SnowflakeUploader(
        connection_config=snowflake_connection_config,
        upload_config=MagicMock(table_name="test_table"),
    )
    uploader._embeddings_dimension = None
    return uploader


@pytest.fixture
def mock_cursor(mocker: MockerFixture):
    return mocker.MagicMock()


@pytest.fixture
def mock_get_cursor(mocker: MockerFixture, mock_cursor: MagicMock):
    mock = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.snowflake.SnowflakeConnectionConfig.get_cursor",
        autospec=True,
    )
    mock.return_value.__enter__.return_value = mock_cursor
    return mock


def test_embeddings_dimension_vector_column(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchone.return_value = (
        "",
        "",
        "",
        '{"type": "VECTOR", "dimension": 128}',
    )

    dimension = snowflake_uploader.embeddings_dimension

    assert dimension == 128
    mock_cursor.execute.assert_called_once_with("SHOW COLUMNS LIKE 'embeddings' IN test_table")


def test_embeddings_dimension_vector_column_dict(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchone.return_value = {
        "col_1": "test_1",
        "col_2": "test_2",
        "data_type": '{"type": "VECTOR", "dimension": 64}',
    }

    dimension = snowflake_uploader.embeddings_dimension

    assert dimension == 64
    mock_cursor.execute.assert_called_once_with("SHOW COLUMNS LIKE 'embeddings' IN test_table")


def test_embeddings_dimension_no_column(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchone.return_value = None

    dimension = snowflake_uploader.embeddings_dimension

    assert dimension == 0
    mock_cursor.execute.assert_called_once_with("SHOW COLUMNS LIKE 'embeddings' IN test_table")


def test_embeddings_dimension_non_vector_column(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchone.return_value = ("", "", "", '{"type": "STRING"}')

    dimension = snowflake_uploader.embeddings_dimension

    assert dimension == 0
    mock_cursor.execute.assert_called_once_with("SHOW COLUMNS LIKE 'embeddings' IN test_table")


def test_parse_select_with_vector_columns(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._embeddings_dimension = 128
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON(?)::VECTOR(FLOAT,128),PARSE_JSON(?),?"
    assert result == expected_result


def test_parse_select_with_vector_columns_embedding_dimension_zero(
    snowflake_uploader: SnowflakeUploader,
):
    snowflake_uploader._embeddings_dimension = 0
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON(?),PARSE_JSON(?),?"
    assert result == expected_result


def test_parse_select_with_vector_columns_embedding_dimension_none(
    mocker: MockerFixture, snowflake_uploader: SnowflakeUploader
):
    mock_embeddings_dimension = mocker.PropertyMock
    mock_embeddings_dimension.return_value = None
    mocker.patch(
        "unstructured_ingest.processes.connectors.sql.snowflake.SnowflakeUploader.embeddings_dimension",
        new_callable=mock_embeddings_dimension,
    )
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON(?),PARSE_JSON(?),?"
    assert result == expected_result
