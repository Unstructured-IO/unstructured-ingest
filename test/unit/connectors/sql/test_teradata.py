from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.processes.connectors.sql.teradata import (
    DEFAULT_TABLE_NAME,
    TeradataAccessConfig,
    TeradataConnectionConfig,
    TeradataDownloader,
    TeradataDownloaderConfig,
    TeradataIndexer,
    TeradataIndexerConfig,
    TeradataUploader,
    TeradataUploaderConfig,
    TeradataUploadStager,
    TeradataUploadStagerConfig,
    _resolve_db_column_case,
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
def teradata_indexer(teradata_connection_config: TeradataConnectionConfig):
    return TeradataIndexer(
        connection_config=teradata_connection_config,
        index_config=TeradataIndexerConfig(
            table_name="year",  # Reserved word as table name
            id_column="type",  # Reserved word as column name
            batch_size=100,
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


def test_teradata_indexer_get_doc_ids_quotes_identifiers(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """Test that _get_doc_ids quotes table and column names to handle reserved words."""
    mock_cursor.fetchall.return_value = [("id1",), ("id2",), ("id3",)]

    teradata_indexer._get_doc_ids()

    # Verify the SELECT statement quotes all identifiers
    call_args = mock_cursor.execute.call_args[0][0]
    assert 'SELECT "type"' in call_args  # Reserved word column quoted
    assert 'FROM "year"' in call_args  # Reserved word table quoted


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

    _, _ = teradata_downloader.query_db(batch_data)

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
    mock_cursor.description = [("id",), ("text",), ("type",), ("record_id",)]
    mock_cursor.rowcount = 5

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.delete_by_record_id(file_data)

    # Last execute call is the DELETE (first is SELECT TOP 1 for column discovery)
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

    teradata_uploader._columns = ["id", "text", "type", "record_id"]
    mocker.patch.object(teradata_uploader, "_fit_to_schema", return_value=df)
    mocker.patch.object(teradata_uploader, "can_delete", return_value=False)

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.upload_dataframe(df, file_data)

    call_args = mock_cursor.executemany.call_args[0][0]
    assert '"id"' in call_args
    assert '"text"' in call_args
    assert '"type"' in call_args
    assert '"record_id"' in call_args
    assert 'INSERT INTO "test_table"' in call_args


def test_teradata_uploader_values_delimiter_is_qmark(teradata_uploader: TeradataUploader):
    """Test that Teradata uses qmark (?) parameter style."""
    assert teradata_uploader.values_delimiter == "?"


def test_teradata_indexer_precheck_success(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """Test that precheck passes when connection and table are valid."""
    teradata_indexer.precheck()

    assert mock_cursor.execute.call_count == 2
    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert calls[0] == "SELECT 1"
    assert calls[1] == 'SELECT TOP 1 * FROM "year"'


def test_teradata_indexer_precheck_connection_failure(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """Test that precheck raises SourceConnectionError when connection fails."""
    mock_cursor.execute.side_effect = Exception("Connection refused")

    with pytest.raises(SourceConnectionError, match="failed to validate connection"):
        teradata_indexer.precheck()

    # Should have only attempted the connectivity check
    assert mock_cursor.execute.call_count == 1


def test_teradata_downloader_query_db_includes_id_column_in_fields(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that query_db includes id_column in SELECT even when fields doesn't contain it."""
    downloader = TeradataDownloader(
        connection_config=teradata_connection_config,
        download_config=TeradataDownloaderConfig(
            fields=["text", "year"],
            id_column="id",
        ),
    )

    mock_cursor.fetchall.return_value = [(1, "text1", 2020)]
    mock_cursor.description = [("id",), ("text",), ("year",)]

    mock_item = MagicMock()
    mock_item.identifier = "test_id"

    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    downloader.query_db(batch_data)

    call_args = mock_cursor.execute.call_args[0][0]
    # id_column should appear in SELECT even though fields was only ["text", "year"]
    assert '"id"' in call_args
    assert '"text"' in call_args
    assert '"year"' in call_args


def test_teradata_downloader_query_db_no_duplicate_when_id_in_fields(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that id_column is not duplicated when it's already in fields."""
    downloader = TeradataDownloader(
        connection_config=teradata_connection_config,
        download_config=TeradataDownloaderConfig(
            fields=["id", "text"],
            id_column="id",
        ),
    )

    mock_cursor.fetchall.return_value = [(1, "text1")]
    mock_cursor.description = [("id",), ("text",)]

    mock_item = MagicMock()
    mock_item.identifier = "test_id"

    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    downloader.query_db(batch_data)

    call_args = mock_cursor.execute.call_args[0][0]
    # "id" should appear exactly once in the SELECT fields
    select_part = call_args.split("FROM")[0]
    assert select_part.count('"id"') == 1


def test_teradata_indexer_precheck_table_not_found(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """Test that precheck raises SourceConnectionError when table doesn't exist."""
    mock_cursor.execute.side_effect = [
        None,  # SELECT 1 succeeds
        Exception("[Error 3807] Object 'year' does not exist"),
    ]

    with pytest.raises(SourceConnectionError, match="Table 'year'.*not found or not accessible"):
        teradata_indexer.precheck()

    assert mock_cursor.execute.call_count == 2


def test_teradata_uploader_precheck_success(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that uploader precheck passes when connection and table are valid."""
    teradata_uploader.precheck()

    assert mock_cursor.execute.call_count == 2
    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert calls[0] == "SELECT 1"
    assert calls[1] == 'SELECT TOP 1 * FROM "test_table"'


def test_teradata_uploader_precheck_connection_failure(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that uploader precheck raises DestinationConnectionError when connection fails."""
    mock_cursor.execute.side_effect = Exception("Connection refused")

    with pytest.raises(DestinationConnectionError, match="failed to validate connection"):
        teradata_uploader.precheck()

    assert mock_cursor.execute.call_count == 1


def test_teradata_uploader_precheck_table_not_found(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that uploader precheck raises DestinationConnectionError when table doesn't exist."""
    mock_cursor.execute.side_effect = [
        None,  # SELECT 1 succeeds
        Exception("[Error 3807] Object 'test_table' does not exist"),
    ]

    with pytest.raises(
        DestinationConnectionError, match="Table 'test_table'.*not found or not accessible"
    ):
        teradata_uploader.precheck()

    assert mock_cursor.execute.call_count == 2


def test_resolve_db_column_case_queries_and_caches(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that _resolve_db_column_case queries on cache miss and returns cached on hit."""
    mock_cursor.description = [("ID",), ("TEXT",), ("TYPE",)]
    cache: dict = {}

    result = _resolve_db_column_case(
        teradata_connection_config.get_cursor,
        "my_table",
        "id",
        cache,
    )
    assert result == "ID"
    assert mock_cursor.execute.call_count == 1
    assert 'SELECT TOP 1 * FROM "my_table"' in mock_cursor.execute.call_args[0][0]

    result2 = _resolve_db_column_case(
        teradata_connection_config.get_cursor,
        "my_table",
        "text",
        cache,
    )
    assert result2 == "TEXT"
    assert mock_cursor.execute.call_count == 1  # no extra query — cache hit


def test_resolve_db_column_case_fallback_on_unknown_column(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that _resolve_db_column_case returns the input when column is not in the table."""
    mock_cursor.description = [("ID",), ("TEXT",)]
    cache: dict = {}

    result = _resolve_db_column_case(
        teradata_connection_config.get_cursor,
        "my_table",
        "nonexistent",
        cache,
    )
    assert result == "nonexistent"


def test_teradata_uploader_get_table_columns_preserves_original_case(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that get_table_columns preserves the original case from cursor.description."""
    mock_cursor.description = [("ID",), ("TEXT",), ("TYPE",)]

    columns = teradata_uploader.get_table_columns()

    assert columns == ["ID", "TEXT", "TYPE"]


def test_teradata_uploader_can_delete_case_insensitive(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that can_delete matches record_id_key case-insensitively against DB columns."""
    mock_cursor.description = [("ID",), ("TEXT",), ("TYPE",), ("RECORD_ID",)]

    assert teradata_uploader.can_delete() is True


def test_teradata_uploader_can_delete_returns_false_when_missing(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that can_delete returns False when record_id_key is not in table columns."""
    mock_cursor.description = [("ID",), ("TEXT",), ("TYPE",)]

    assert teradata_uploader.can_delete() is False


def test_teradata_uploader_delete_by_record_id_resolves_column_case(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that delete_by_record_id resolves the column name to actual DB case."""
    mock_cursor.description = [("ID",), ("TEXT",), ("TYPE",), ("RECORD_ID",)]
    mock_cursor.rowcount = 3

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.delete_by_record_id(file_data)

    call_args = mock_cursor.execute.call_args[0][0]
    assert 'DELETE FROM "test_table"' in call_args
    assert 'WHERE "RECORD_ID" = ?' in call_args


def test_teradata_uploader_upload_dataframe_uses_db_case_in_sql(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that upload_dataframe uses actual DB column case in quoted SQL identifiers."""
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "text": ["text1", "text2"],
            "type": ["Title", "NarrativeText"],
            "record_id": ["file1", "file1"],
        }
    )

    teradata_uploader._columns = ["ID", "TEXT", "TYPE", "RECORD_ID"]
    mocker.patch.object(teradata_uploader, "_fit_to_schema", return_value=df)
    mocker.patch.object(teradata_uploader, "can_delete", return_value=False)

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    teradata_uploader.upload_dataframe(df, file_data)

    call_args = mock_cursor.executemany.call_args[0][0]
    assert '"ID"' in call_args
    assert '"TEXT"' in call_args
    assert '"TYPE"' in call_args
    assert '"RECORD_ID"' in call_args
    assert 'INSERT INTO "test_table"' in call_args


def test_teradata_downloader_query_db_lowercases_uppercase_columns(
    mock_cursor: MagicMock,
    teradata_downloader: TeradataDownloader,
    mock_get_cursor: MagicMock,
):
    """Test that query_db normalizes uppercase cursor.description columns to lowercase."""
    mock_cursor.fetchall.return_value = [
        (1, "text1", 2020),
    ]
    mock_cursor.description = [("ID",), ("TEXT",), ("YEAR",)]

    mock_item = MagicMock()
    mock_item.identifier = "test_id"

    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    _, columns = teradata_downloader.query_db(batch_data)

    assert columns == ["id", "text", "year"]


def test_indexer_with_uppercase_enterprise_columns(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """Test that _get_doc_ids uses resolved uppercase column names."""
    teradata_indexer._column_cache["year"] = {"type": "TYPE", "id": "ID"}
    mock_cursor.fetchall.return_value = [("id1",), ("id2",)]

    teradata_indexer._get_doc_ids()

    call_args = mock_cursor.execute.call_args[0][0]
    assert 'SELECT "TYPE" FROM "year"' in call_args


def test_downloader_with_uppercase_enterprise_columns(
    mock_cursor: MagicMock,
    teradata_downloader: TeradataDownloader,
    mock_get_cursor: MagicMock,
):
    """Test that query_db resolves uppercase column names."""
    teradata_downloader._column_cache["elements"] = {
        "id": "ID",
        "text": "TEXT",
        "year": "YEAR",
        "record_id": "RECORD_ID",
    }
    mock_cursor.fetchall.return_value = [(1, "text1", 2020)]
    mock_cursor.description = [("ID",), ("TEXT",), ("YEAR",)]

    mock_item = MagicMock()
    mock_item.identifier = "test_id"
    batch_data = MagicMock()
    batch_data.additional_metadata.table_name = "elements"
    batch_data.additional_metadata.id_column = "id"
    batch_data.batch_items = [mock_item]

    _, columns = teradata_downloader.query_db(batch_data)

    call_args = mock_cursor.execute.call_args[0][0]
    assert 'FROM "elements"' in call_args
    assert 'WHERE "ID" IN' in call_args
    assert '"ID"' in call_args and '"TEXT"' in call_args and '"YEAR"' in call_args
    assert columns == ["id", "text", "year"]


def test_teradata_connection_close_called_when_commit_fails(
    mocker: MockerFixture,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that connection.close() is always called even when commit() raises."""
    mock_conn = MagicMock()
    mock_conn.commit.side_effect = Exception("commit failed")

    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn
    mocker.patch.dict("sys.modules", {"teradatasql": mock_module})

    with (
        pytest.raises(Exception, match="commit failed"),
        teradata_connection_config.get_connection(),
    ):
        pass

    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


def test_teradata_connection_close_called_when_operation_and_commit_both_fail(
    mocker: MockerFixture,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Test that connection.close() is called when both the operation and commit() raise."""
    mock_conn = MagicMock()
    mock_conn.commit.side_effect = Exception("commit failed")

    mock_module = MagicMock()
    mock_module.connect.return_value = mock_conn
    mocker.patch.dict("sys.modules", {"teradatasql": mock_module})

    with pytest.raises(Exception), teradata_connection_config.get_connection():
        raise RuntimeError("operation failed")

    mock_conn.close.assert_called_once()


# --- Opinionated writes (metadata_as_json) tests ---


@pytest.fixture
def teradata_upload_stager_json():
    return TeradataUploadStager(
        upload_stager_config=TeradataUploadStagerConfig(metadata_as_json=True)
    )


@pytest.fixture
def teradata_uploader_auto_create(teradata_connection_config: TeradataConnectionConfig):
    return TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(),
    )


def test_teradata_uploader_config_table_name_defaults_to_none():
    config = TeradataUploaderConfig()
    assert config.table_name is None


def test_teradata_stager_config_metadata_as_json_defaults_to_false():
    config = TeradataUploadStagerConfig()
    assert config.metadata_as_json is False


def test_teradata_stager_conform_dict_json_mode(
    teradata_upload_stager_json: TeradataUploadStager,
):
    """When metadata_as_json=True, conform_dict produces the 6-column JSON blob shape."""
    element_dict = {
        "element_id": "abc123",
        "text": "Hello world",
        "type": "NarrativeText",
        "metadata": {
            "filename": "test.pdf",
            "filetype": "application/pdf",
            "languages": ["en"],
        },
    }
    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    result = teradata_upload_stager_json.conform_dict(
        element_dict=element_dict, file_data=file_data
    )

    assert set(result.keys()) == {"id", "record_id", "element_id", "text", "type", "metadata"}
    assert result["element_id"] == "abc123"
    assert result["text"] == "Hello world"
    assert result["type"] == "NarrativeText"
    assert result["record_id"] == "test_file.txt"
    assert isinstance(result["metadata"], str)
    metadata_parsed = __import__("json").loads(result["metadata"])
    assert metadata_parsed["filename"] == "test.pdf"
    assert metadata_parsed["languages"] == ["en"]


def test_teradata_stager_conform_dict_flattened_mode(
    teradata_upload_stager: TeradataUploadStager,
):
    """When metadata_as_json=False, conform_dict delegates to base (flattened metadata)."""
    element_dict = {
        "element_id": "abc123",
        "text": "Hello world",
        "type": "NarrativeText",
        "metadata": {
            "filename": "test.pdf",
            "filetype": "application/pdf",
            "data_source": {"url": "http://example.com"},
        },
    }
    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    result = teradata_upload_stager.conform_dict(
        element_dict=element_dict, file_data=file_data
    )

    assert "filename" in result
    assert "filetype" in result
    assert "url" in result
    assert "metadata" not in result


def test_teradata_stager_conform_dataframe_json_mode_is_passthrough(
    teradata_upload_stager_json: TeradataUploadStager,
):
    """When metadata_as_json=True, conform_dataframe returns the df unchanged."""
    df = pd.DataFrame(
        {
            "id": ["id1"],
            "record_id": ["rec1"],
            "element_id": ["el1"],
            "text": ["Hello"],
            "type": ["NarrativeText"],
            "metadata": ['{"filename": "test.pdf"}'],
        }
    )

    result = teradata_upload_stager_json.conform_dataframe(df)

    pd.testing.assert_frame_equal(result, df)


def test_teradata_uploader_precheck_skips_table_check_when_table_name_is_none(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """When table_name is None, precheck only validates connection, not table existence."""
    teradata_uploader_auto_create.precheck()

    assert mock_cursor.execute.call_count == 1
    assert mock_cursor.execute.call_args[0][0] == "SELECT 1"


def test_teradata_uploader_create_destination_creates_table_when_missing(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """create_destination creates the table when it doesn't exist in DBC.TablesV."""
    mock_cursor.fetchone.return_value = None

    result = teradata_uploader_auto_create.create_destination()

    assert result is True
    assert teradata_uploader_auto_create.upload_config.table_name == DEFAULT_TABLE_NAME

    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert any("DBC.TablesV" in c for c in calls)
    assert any("CREATE MULTISET TABLE" in c for c in calls)


def test_teradata_uploader_create_destination_skips_when_table_exists(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """create_destination returns False when the table already exists."""
    mock_cursor.fetchone.return_value = (1,)

    result = teradata_uploader_auto_create.create_destination()

    assert result is False
    assert teradata_uploader_auto_create.upload_config.table_name == DEFAULT_TABLE_NAME
    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert not any("CREATE" in c for c in calls)


def test_teradata_uploader_create_destination_uses_provided_table_name(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """create_destination prefers upload_config.table_name over the default."""
    uploader = TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(table_name="my_custom_table"),
    )
    mock_cursor.fetchone.return_value = None

    result = uploader.create_destination()

    assert result is True
    assert uploader.upload_config.table_name == "my_custom_table"
    create_call = [c[0][0] for c in mock_cursor.execute.call_args_list if "CREATE" in c[0][0]]
    assert len(create_call) == 1
    assert "my_custom_table" in create_call[0]


def test_teradata_uploader_create_destination_uses_destination_name_kwarg(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """create_destination uses destination_name kwarg when table_name is None."""
    mock_cursor.fetchone.return_value = None

    result = teradata_uploader_auto_create.create_destination(
        destination_name="workflow_123"
    )

    assert result is True
    assert teradata_uploader_auto_create.upload_config.table_name == "workflow_123"
