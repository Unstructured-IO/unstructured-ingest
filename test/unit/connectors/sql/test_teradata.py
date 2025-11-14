from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.sql.teradata import (
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataDownloader,
    TeradataDownloaderConfig,
    TeradataUploader,
    TeradataUploaderConfig,
    TeradataUploadStager,
)


@pytest.fixture
def teradata_access_config():
    return TeradataAccessConfig(password="test_password")


@pytest.fixture
def teradata_connection_config(teradata_access_config: TeradataAccessConfig):
    return TeradataConnectionConfig(
        host="test-host.teradata.com",
        user="test_user",
        database="test_db",
        dbs_port=1025,
        access_config=Secret(teradata_access_config),
    )


@pytest.fixture
def teradata_uploader(teradata_connection_config: TeradataConnectionConfig):
    return TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(table_name="test_table", record_id_key="record_id"),
    )


@pytest.fixture
def teradata_downloader(teradata_connection_config: TeradataConnectionConfig):
    return TeradataDownloader(
        connection_config=teradata_connection_config,
        download_config=TeradataDownloaderConfig(
            fields=["id", "text", "year"],
            id_column="id",
        ),
    )


@pytest.fixture
def teradata_upload_stager():
    return TeradataUploadStager()


@pytest.fixture
def mock_cursor(mocker: MockerFixture):
    return mocker.MagicMock()


@pytest.fixture
def mock_get_cursor(mocker: MockerFixture, mock_cursor: MagicMock):
    mock = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.teradata.TeradataConnectionConfig.get_cursor",
        autospec=True,
    )
    mock.return_value.__enter__.return_value = mock_cursor
    return mock


def test_teradata_connection_config_with_database(teradata_access_config: TeradataAccessConfig):
    config = TeradataConnectionConfig(
        host="test-host.teradata.com",
        user="test_user",
        database="my_database",
        dbs_port=1025,
        access_config=Secret(teradata_access_config),
    )
    assert config.database == "my_database"
    assert config.dbs_port == 1025


def test_teradata_connection_config_default_port(teradata_access_config: TeradataAccessConfig):
    config = TeradataConnectionConfig(
        host="test-host.teradata.com",
        user="test_user",
        access_config=Secret(teradata_access_config),
    )
    assert config.dbs_port == 1025
    assert config.database is None


def test_teradata_downloader_query_db_quotes_identifiers(
    mock_cursor: MagicMock,
    teradata_downloader: TeradataDownloader,
    mock_get_cursor: MagicMock,
):
    """Test that query_db quotes all table and column names to handle reserved words."""
    mock_cursor.fetchall.return_value = [
        (1, "text1", 2020),
        (2, "text2", 2021),
    ]
    mock_cursor.description = [("id",), ("text",), ("year",)]

    # Create proper mock structure for SqlBatchFileData
    mock_item = MagicMock()
    mock_item.identifier = "test_id"

    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    results, columns = teradata_downloader.query_db(batch_data)

    # Verify the SELECT statement quotes all identifiers
    call_args = mock_cursor.execute.call_args[0][0]
    assert '"id"' in call_args  # Field name quoted
    assert '"text"' in call_args  # Field name quoted
    assert '"year"' in call_args  # Reserved word field quoted
    assert '"elements"' in call_args  # Table name quoted
    # Verify WHERE clause also quotes the id column
    assert 'WHERE "id" IN' in call_args


def test_teradata_downloader_query_db_returns_correct_data(
    mock_cursor: MagicMock,
    teradata_downloader: TeradataDownloader,
    mock_get_cursor: MagicMock,
):
    """Test that query_db returns data in the expected format."""
    mock_cursor.fetchall.return_value = [
        (1, "text1", 2020),
        (2, "text2", 2021),
    ]
    mock_cursor.description = [("id",), ("text",), ("year",)]

    # Create proper mock structure for SqlBatchFileData
    mock_item = MagicMock()
    mock_item.identifier = "test_id"

    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    results, columns = teradata_downloader.query_db(batch_data)

    assert results == [(1, "text1", 2020), (2, "text2", 2021)]
    assert columns == ["id", "text", "year"]


def test_teradata_upload_stager_converts_lists_to_json(
    teradata_upload_stager: TeradataUploadStager,
):
    """Test that conform_dataframe converts Python lists to JSON strings."""
    df = pd.DataFrame(
        {
            "text": ["text1", "text2"],
            "languages": [["en"], ["en", "fr"]],
            "id": [1, 2],
        }
    )

    result = teradata_upload_stager.conform_dataframe(df)

    # languages column should be JSON strings now
    assert isinstance(result["languages"].iloc[0], str)
    assert result["languages"].iloc[0] == '["en"]'
    assert result["languages"].iloc[1] == '["en", "fr"]'
    # Other columns should be unchanged
    assert result["text"].iloc[0] == "text1"
    assert result["id"].iloc[0] == 1


def test_teradata_upload_stager_converts_dicts_to_json(
    teradata_upload_stager: TeradataUploadStager,
):
    """Test that conform_dataframe converts Python dicts to JSON strings."""
    df = pd.DataFrame(
        {
            "text": ["text1", "text2"],
            "metadata": [{"key": "value1"}, {"key": "value2"}],
            "id": [1, 2],
        }
    )

    result = teradata_upload_stager.conform_dataframe(df)

    # metadata column should be JSON strings now
    assert isinstance(result["metadata"].iloc[0], str)
    assert result["metadata"].iloc[0] == '{"key": "value1"}'
    assert result["metadata"].iloc[1] == '{"key": "value2"}'


def test_teradata_upload_stager_handles_empty_dataframe(
    teradata_upload_stager: TeradataUploadStager,
):
    """Test that conform_dataframe handles empty DataFrames."""
    df = pd.DataFrame({"text": [], "languages": []})

    result = teradata_upload_stager.conform_dataframe(df)

    assert len(result) == 0
    assert "text" in result.columns
    assert "languages" in result.columns


def test_teradata_upload_stager_handles_none_values(
    teradata_upload_stager: TeradataUploadStager,
):
    """Test that conform_dataframe handles None values in list/dict columns."""
    df = pd.DataFrame(
        {
            "text": ["text1", "text2"],
            "languages": [["en"], None],
        }
    )

    result = teradata_upload_stager.conform_dataframe(df)

    # First row should be JSON string, second should be None
    assert result["languages"].iloc[0] == '["en"]'
    assert pd.isna(result["languages"].iloc[1])


def test_teradata_uploader_get_table_columns_uses_top_syntax(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that get_table_columns uses Teradata's TOP syntax instead of LIMIT."""
    mock_cursor.description = [("id",), ("text",), ("type",)]

    columns = teradata_uploader.get_table_columns()

    # Verify the query uses TOP instead of LIMIT
    call_args = mock_cursor.execute.call_args[0][0]
    assert "SELECT TOP 1" in call_args
    assert "LIMIT" not in call_args
    assert columns == ["id", "text", "type"]


def test_teradata_uploader_delete_by_record_id_quotes_identifiers(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that delete_by_record_id quotes table and column names."""
    mock_cursor.rowcount = 5

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.delete_by_record_id(file_data)

    # Verify the DELETE statement quotes identifiers
    call_args = mock_cursor.execute.call_args[0][0]
    assert 'DELETE FROM "test_table"' in call_args
    assert 'WHERE "record_id" = ?' in call_args


def test_teradata_uploader_upload_dataframe_quotes_column_names(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that upload_dataframe quotes all column names in INSERT statement."""
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "text": ["text1", "text2"],
            "type": ["Title", "NarrativeText"],
            "record_id": ["file1", "file1"],
        }
    )

    # Mock _fit_to_schema to return the same df
    mocker.patch.object(teradata_uploader, "_fit_to_schema", return_value=df)
    # Mock can_delete to return False
    mocker.patch.object(teradata_uploader, "can_delete", return_value=False)

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.upload_dataframe(df, file_data)

    # Verify the INSERT statement quotes all column names
    call_args = mock_cursor.executemany.call_args[0][0]
    assert '"id"' in call_args
    assert '"text"' in call_args
    assert '"type"' in call_args  # Reserved word must be quoted
    assert '"record_id"' in call_args
    assert "INSERT INTO test_table" in call_args


def test_teradata_uploader_values_delimiter_is_qmark(teradata_uploader: TeradataUploader):
    """Test that Teradata uses qmark (?) parameter style."""
    assert teradata_uploader.values_delimiter == "?"
