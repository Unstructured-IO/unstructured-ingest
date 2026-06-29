from unittest.mock import MagicMock

import pandas as pd
import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    UserError,
)
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
    _build_proxy_params,
    _extract_teradata_error_code,
    _is_teradata_driver_error,
    _raise_classified_teradata_error,
    _resolve_db_column_case,
    _summarize_error,
)


# --- Test helper -------------------------------------------------------------
# Many tests below need to raise an exception that the new
# `_is_teradata_driver_error` filter accepts. Real `teradatasql.OperationalError`
# isn't importable without the (optional) teradatasql package installed in the
# test env, so we forge a stand-in by overriding ``__module__``.
class _FakeTeradataDriverError(Exception):
    """Stand-in for a teradatasql driver exception (same module signature)."""


_FakeTeradataDriverError.__module__ = "teradatasql"


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


def test_teradata_upload_stager_conform_dataframe_embeddings_as_csv(
    teradata_upload_stager: TeradataUploadStager,
):
    """Test that conform_dataframe converts embeddings lists to comma-separated floats."""
    df = pd.DataFrame(
        {
            "text": ["text1", "text2"],
            "embeddings": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
            "languages": [["en"], ["fr"]],
            "id": [1, 2],
        }
    )

    result = teradata_upload_stager.conform_dataframe(df)

    # embeddings should be comma-separated floats, not JSON
    assert result["embeddings"].iloc[0] == "0.1,0.2,0.3"
    assert result["embeddings"].iloc[1] == "0.4,0.5,0.6"
    # other list columns should still be JSON
    assert result["languages"].iloc[0] == '["en"]'


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


@pytest.mark.parametrize(
    ("error_msg", "expected_suffix"),
    [
        ("dial tcp 192.168.1.1:1025: i/o timeout", "connection timed out"),
        ("connection timed out after 10000 ms", "connection timed out"),
        ("Connection refused", "connection refused"),
        ("no route to host", "connection refused"),
        # Teradata Go driver embeds "[Logon]" phase even on connection errors
        ("[Logon] dial tcp: connection refused", "connection refused"),
        # TCP-level connection reset should still match
        ("connection reset by peer", "connection refused"),
        ("authentication failed for user foo", "invalid credentials"),
        ("Logon failed", "invalid credentials"),
        # "password has been reset" must NOT match connection-refused
        ("password has been reset", "invalid credentials"),
        ("some unknown driver error", None),
    ],
)
def test_summarize_error(error_msg: str, expected_suffix: str | None):
    result = _summarize_error("myhost", Exception(error_msg))
    assert result.startswith("Failed to connect") or result.startswith("Failed to authenticate")
    assert "myhost" in result
    if expected_suffix:
        assert expected_suffix in result


def test_summarize_error_context_overrides_regex():
    """Context param takes priority over regex and uses a neutral prefix."""
    result = _summarize_error(
        "myhost",
        Exception("[Error 3807] Object 'password_reset_log' does not exist"),
        context="table 'password_reset_log' not found or not accessible",
    )
    # Must NOT say "Failed to connect" — the connection succeeded
    assert not result.startswith("Failed to connect")
    assert "myhost" in result
    assert "table 'password_reset_log' not found or not accessible" in result


def test_build_proxy_params_empty(monkeypatch):
    """No proxy env vars → empty dict."""
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    assert _build_proxy_params() == {}


def test_build_proxy_params_https_only(monkeypatch):
    """HTTPS_PROXY alone is forwarded as https_proxy."""
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.com:8080")
    for var in ("https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    params = _build_proxy_params()
    assert params["https_proxy"] == "http://proxy.example.com:8080"
    assert "http_proxy" not in params
    assert "proxy_bypass_hosts" not in params


def test_build_proxy_params_lowercase_wins(monkeypatch):
    """Lowercase env var is used when uppercase is absent."""
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.setenv("https_proxy", "http://lower.proxy:9000")
    for var in ("HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    params = _build_proxy_params()
    assert params["https_proxy"] == "http://lower.proxy:9000"


def test_build_proxy_params_no_proxy_conversion(monkeypatch):
    """NO_PROXY is converted: commas→pipes, leading dots→wildcards, CIDRs skipped."""
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:912")
    monkeypatch.setenv(
        "NO_PROXY",
        "localhost,127.0.0.1,10.0.0.0/8,.svc,.svc.cluster.local,kubernetes.default",
    )
    for var in ("https_proxy", "HTTP_PROXY", "http_proxy", "no_proxy"):
        monkeypatch.delenv(var, raising=False)
    params = _build_proxy_params()
    bypass = params["proxy_bypass_hosts"]
    assert "localhost" in bypass
    assert "127.0.0.1" in bypass
    assert "*.svc" in bypass
    assert "*.svc.cluster.local" in bypass
    assert "kubernetes.default" in bypass
    # CIDR entry must be dropped
    assert "10.0.0.0/8" not in bypass
    assert "10.0.0.0" not in bypass
    # Separator must be pipe
    assert "|" in bypass
    assert "," not in bypass


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

    with pytest.raises(
        SourceConnectionError, match="Failed to connect to server.*connection refused"
    ):
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


def test_teradata_indexer_precheck_table_not_found_raises_user_error(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """When Teradata returns [Error 3807] the indexer precheck raises a UserError
    that names the table and forwards the raw TD message — so the customer can
    self-serve the fix instead of seeing 'Failed to connect…'."""
    mock_cursor.execute.side_effect = [
        None,  # SELECT 1 succeeds
        _FakeTeradataDriverError("[Error 3807] Object 'year' does not exist"),
    ]

    with pytest.raises(UserError, match=r"3807.*does not exist or user has no privilege.*'year'"):
        teradata_indexer.precheck()

    assert mock_cursor.execute.call_count == 2


def test_teradata_indexer_precheck_table_unknown_error_falls_back_to_source_error(
    mock_cursor: MagicMock,
    teradata_indexer: TeradataIndexer,
    mock_get_cursor: MagicMock,
):
    """A table-probe failure with a teradatasql-shaped exception but no recognised
    TD error code falls back to SourceConnectionError AND preserves the historical
    "table 'X' not found or not accessible" context message."""
    mock_cursor.execute.side_effect = [
        None,  # SELECT 1 succeeds
        _FakeTeradataDriverError("some unexpected driver error without a [Error N] tag"),
    ]

    with pytest.raises(SourceConnectionError, match=r"table 'year'.*not found or not accessible"):
        teradata_indexer.precheck()


def test_teradata_uploader_precheck_success(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that uploader precheck only validates connection, not table existence."""
    teradata_uploader.precheck()

    assert mock_cursor.execute.call_count == 1
    assert mock_cursor.execute.call_args[0][0] == "SELECT 1"


def test_teradata_uploader_precheck_connection_failure(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Test that uploader precheck raises DestinationConnectionError when connection fails."""
    mock_cursor.execute.side_effect = Exception("Connection refused")

    with pytest.raises(
        DestinationConnectionError, match="Failed to connect to server.*connection refused"
    ):
        teradata_uploader.precheck()

    assert mock_cursor.execute.call_count == 1


def test_teradata_uploader_precheck_does_not_check_table(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Precheck never checks table existence; create_destination handles missing tables."""
    teradata_uploader.precheck()

    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert calls == ["SELECT 1"]
    assert not any("SELECT TOP" in c for c in calls)


def test_teradata_uploader_config_preserves_user_table_name_for_precheck(
    teradata_connection_config: TeradataConnectionConfig,
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
):
    """User-provided table names are not modified; precheck passes them through as-is."""
    uploader = TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(
            table_name="my-bad-table", record_id_key="record_id"
        ),
    )
    assert uploader.upload_config.table_name == "my-bad-table"
    uploader.precheck()  # must not raise


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


def test_teradata_upload_stager_conform_dict_includes_embeddings():
    """Test that conform_dict includes embeddings when metadata_as_json=True."""
    stager = TeradataUploadStager(
        upload_stager_config=TeradataUploadStagerConfig(metadata_as_json=True),
    )
    element_dict = {
        "element_id": "abc123",
        "text": "hello world",
        "type": "NarrativeText",
        "embeddings": [0.1, 0.2, 0.3],
        "metadata": {"page_number": 1},
    }
    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    result = stager.conform_dict(element_dict=element_dict, file_data=file_data)

    assert "embeddings" in result
    assert result["embeddings"] == "0.1,0.2,0.3"
    assert result["text"] == "hello world"
    assert result["type"] == "NarrativeText"


def test_teradata_upload_stager_conform_dict_embeddings_null_when_absent():
    """Test that conform_dict sets embeddings to None when not present in element."""
    stager = TeradataUploadStager(
        upload_stager_config=TeradataUploadStagerConfig(metadata_as_json=True),
    )
    element_dict = {
        "element_id": "abc123",
        "text": "hello world",
        "type": "NarrativeText",
        "metadata": {"page_number": 1},
    }
    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    result = stager.conform_dict(element_dict=element_dict, file_data=file_data)

    assert "embeddings" in result
    assert result["embeddings"] is None


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


def test_teradata_uploader_config_preserves_user_table_name_with_dashes():
    config = TeradataUploaderConfig(table_name="my-table-name")
    assert config.table_name == "my-table-name"


def test_teradata_uploader_config_accepts_underscored_table_name():
    config = TeradataUploaderConfig(table_name="my_table_name")
    assert config.table_name == "my_table_name"


def test_teradata_stager_config_metadata_as_json_defaults_to_false():
    config = TeradataUploadStagerConfig()
    assert config.metadata_as_json is False


def test_teradata_stager_conform_dict_json_mode(
    teradata_upload_stager_json: TeradataUploadStager,
):
    """When metadata_as_json=True, conform_dict produces the opinionated column shape."""
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

    assert set(result.keys()) == {
        "id", "record_id", "element_id", "text", "type", "embeddings", "metadata",
    }
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

    result = teradata_upload_stager.conform_dict(element_dict=element_dict, file_data=file_data)

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


def test_teradata_uploader_precheck_with_table_name_none(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Precheck validates connection only, regardless of table_name being None."""
    teradata_uploader_auto_create.precheck()

    assert mock_cursor.execute.call_count == 1
    assert mock_cursor.execute.call_args[0][0] == "SELECT 1"


def test_teradata_uploader_create_destination_creates_table_when_missing(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """create_destination creates the table when it doesn't exist in DBC.TablesV."""
    mock_cursor.fetchone.side_effect = [("test_db",), None]

    result = teradata_uploader_auto_create.create_destination()

    assert result is True
    assert teradata_uploader_auto_create.upload_config.table_name == DEFAULT_TABLE_NAME

    calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert "SELECT DATABASE" in calls
    dbc_call = [c for c in calls if "DBC.TablesV" in c]
    assert len(dbc_call) == 1
    assert "DatabaseName = ?" in dbc_call[0]
    assert any("CREATE MULTISET TABLE" in c for c in calls)


def test_teradata_uploader_create_destination_skips_when_table_exists(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """create_destination returns False when the table already exists."""
    mock_cursor.fetchone.side_effect = [("test_db",), (1,)]

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
    mock_cursor.fetchone.side_effect = [("test_db",), None]

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
    mock_cursor.fetchone.side_effect = [("test_db",), None]

    result = teradata_uploader_auto_create.create_destination(destination_name="workflow_123")

    assert result is True
    assert teradata_uploader_auto_create.upload_config.table_name == "workflow_123"


def test_teradata_uploader_create_destination_sanitizes_dashes_in_destination_name(
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
    mock_cursor: MagicMock,
):
    """create_destination sanitizes dashes in destination_name by replacing with underscores."""
    mock_cursor.fetchone.side_effect = [("test_db",), None]

    result = teradata_uploader_auto_create.create_destination(destination_name="my-bad-table")

    assert result is True
    assert teradata_uploader_auto_create.upload_config.table_name == "my_bad_table"


def test_teradata_uploader_create_destination_preserves_user_table_name_with_dashes(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """create_destination does not modify a table_name the user explicitly provided."""
    uploader = TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(table_name="my-table"),
    )
    mock_cursor.fetchone.side_effect = [("test_db",), None]

    uploader.create_destination()

    assert uploader.upload_config.table_name == "my-table"


# --- Regression tests for Error 3939 (parameter count mismatch) ---


def test_split_dataframe_no_empty_chunk_when_length_is_multiple_of_chunk_size():
    """split_dataframe must not yield an empty trailing chunk when len(df) % chunk_size == 0.

    The old formula (num_chunks = len(df)//chunk_size + 1) always appended one
    extra iteration, producing an empty df slice that caused teradatasql to send
    a zero-row parameterized batch to the server (Error 3939).
    """
    from unstructured_ingest.utils.data_prep import split_dataframe

    df = pd.DataFrame({"a": range(100)})
    chunks = list(split_dataframe(df, chunk_size=100))
    assert len(chunks) == 1, "exactly one chunk expected for len==chunk_size"
    assert len(chunks[0]) == 100
    assert all(len(c) > 0 for c in chunks), "no empty chunks allowed"


def test_split_dataframe_no_empty_chunk_for_exact_multiples():
    """Verify no empty chunks for several exact multiples of chunk_size."""
    from unstructured_ingest.utils.data_prep import split_dataframe

    for n in (50, 100, 150, 200):
        df = pd.DataFrame({"x": range(n)})
        chunks = list(split_dataframe(df, chunk_size=50))
        assert all(len(c) > 0 for c in chunks), f"empty chunk for n={n}"
        assert sum(len(c) for c in chunks) == n


# ─── Teradata error-code classification ──────────────────────────────────────
#
# The legacy SQL connector used to swallow Teradata driver exceptions into a
# generic "Failed to connect" / "Unexpected job failure" wrapper, hiding the
# actual server message. These tests cover the new typed-error mapping so a
# customer config mistake (missing table, missing privilege, bad SQL) surfaces
# as a UserError carrying the raw TD message — matching the PLU-337 pattern
# already in place for the teradata_vector_v2 connector.
#
# (See _FakeTeradataDriverError near the top of this file for why we forge a
# module-name signature instead of importing teradatasql.OperationalError.)


def test_is_teradata_driver_error_module_check():
    """Module-name filter accepts teradatasql exceptions and rejects others."""
    assert _is_teradata_driver_error(_FakeTeradataDriverError("x")) is True
    assert _is_teradata_driver_error(MemoryError("oom")) is False
    assert _is_teradata_driver_error(OSError("io")) is False
    assert _is_teradata_driver_error(Exception("generic")) is False


@pytest.mark.parametrize(
    ("message", "expected_code"),
    [
        ("[Error 3807] Object 'abc_123' does not exist", 3807),
        ("[Version 20.0.0.54] [Session 11529] [Teradata Database] [Error 3807] x", 3807),
        ("[Error 3523] User does not have CREATE TABLE privilege", 3523),
        ("dial tcp 10.0.0.1:1025: i/o timeout", None),
        ("", None),
    ],
)
def test_extract_teradata_error_code(message: str, expected_code: int | None):
    assert _extract_teradata_error_code(Exception(message)) == expected_code


@pytest.mark.parametrize(
    ("code", "fragment"),
    [
        (3807, "does not exist or user has no privilege"),
        (3523, "user does not have the required privilege"),
        (3706, "SQL syntax error"),
        (3707, "SQL syntax error"),
        (3753, "floating-point overflow during implicit conversion"),
        (3754, "implicit type conversion failed"),
        (5612, "user does not have any access to the object"),
        (5315, "user does not have any access to the database"),
    ],
)
def test_classified_teradata_error_user_fault_codes_raise_user_error(
    code: int, fragment: str
):
    """Recognised user-fault codes → UserError with the descriptor and raw message."""
    exc = _FakeTeradataDriverError(f"[Error {code}] something bad happened")
    with pytest.raises(UserError) as excinfo:
        _raise_classified_teradata_error(exc, host="td.example.com", table="my_table")
    msg = str(excinfo.value)
    assert str(code) in msg
    assert fragment in msg
    assert "'my_table'" in msg
    assert "Teradata reported" in msg
    # Original exception is chained so the driver traceback survives in logs.
    assert excinfo.value.__cause__ is exc


def test_classified_teradata_error_extract_picks_last_code_in_chain():
    """When a driver message chains multiple [Error N] tags, the LAST (most
    specific / innermost) is what gets classified."""
    exc = _FakeTeradataDriverError("[Error 9999] outer wrapper; caused by [Error 3754] inner")
    assert _extract_teradata_error_code(exc) == 3754


def test_classified_teradata_error_unknown_code_destination_fallback():
    """Unknown code on a destination path → DestinationConnectionError preserved."""
    exc = _FakeTeradataDriverError("[Error 9999] something we don't classify yet")
    with pytest.raises(DestinationConnectionError):
        _raise_classified_teradata_error(exc, host="td.example.com", table="t")


def test_classified_teradata_error_unknown_code_source_fallback():
    """Unknown code on a source path → SourceConnectionError preserved."""
    exc = _FakeTeradataDriverError("connection reset by peer")
    with pytest.raises(SourceConnectionError):
        _raise_classified_teradata_error(
            exc, host="td.example.com", table="t", direction="source"
        )


def test_classified_teradata_error_no_code_falls_back():
    """Driver exception without an [Error NNNN] tag (network blip etc.) falls back."""
    exc = _FakeTeradataDriverError("dial tcp 10.0.0.1:1025: i/o timeout")
    with pytest.raises(DestinationConnectionError):
        _raise_classified_teradata_error(exc, host="td.example.com")


def test_classified_teradata_error_rejects_invalid_direction():
    """Runtime check on direction prevents Literal-only typos (e.g.
    direction='DESTINATION' uppercase) from silently falling into the wrong path."""
    exc = _FakeTeradataDriverError("[Error 9999]")
    with pytest.raises(AssertionError, match="direction must be"):
        _raise_classified_teradata_error(
            exc, host="td.example.com", direction="DESTINATION"  # type: ignore[arg-type]
        )


def test_classified_teradata_error_fallback_context_is_forwarded():
    """fallback_context flows into _summarize_error so callers can preserve
    historical per-site messages (e.g. the indexer's 'table X not found')."""
    exc = _FakeTeradataDriverError("[Error 9999] unknown code")
    with pytest.raises(SourceConnectionError, match=r"table 'year'.*not found or not accessible"):
        _raise_classified_teradata_error(
            exc,
            host="td.example.com",
            table="year",
            direction="source",
            fallback_context="table 'year' not found or not accessible",
        )


def test_teradata_uploader_get_table_columns_missing_table_raises_user_error(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """get_table_columns translates Teradata 3807 into a UserError instead of
    letting the bare driver exception bubble through the etl-api retry wrapper."""
    mock_cursor.execute.side_effect = _FakeTeradataDriverError(
        "[Error 3807] Object 'test_table' does not exist"
    )
    teradata_uploader._columns = None

    with pytest.raises(UserError, match="3807.*'test_table'"):
        teradata_uploader.get_table_columns()


def test_teradata_uploader_get_table_columns_non_teradata_error_is_reraised(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Non-teradata exceptions (e.g. OSError, MemoryError) must bubble up
    unchanged — only teradatasql driver errors get reclassified."""
    mock_cursor.execute.side_effect = MemoryError("simulated OOM during fetch")
    teradata_uploader._columns = None

    with pytest.raises(MemoryError):
        teradata_uploader.get_table_columns()


def test_teradata_uploader_delete_by_record_id_type_mismatch_raises_user_error(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """Reproduces the production scenario: customer created abc_123 with record_id
    BIGINT instead of VARCHAR. Connector sends a string identifier, Teradata returns
    [Error 3754]. Without this mapping the customer sees 'Failed to connect…' (wrong);
    with it they see the actual type-mismatch message."""
    mocker.patch.object(
        teradata_uploader, "_get_db_column_name", return_value="record_id"
    )
    mock_cursor.execute.side_effect = _FakeTeradataDriverError(
        "[Error 3754] Precision error in FLOAT type constant or during implicit conversions."
    )

    file_data = FileData(
        identifier="1xdNXwlv2yuGJyUMzQgmtdlAOesAP673T",  # Google Drive file id (string)
        connector_type="google_drive",
        source_identifiers=SourceIdentifiers(
            filename="report.pdf", fullpath="ofc_data1/report.pdf"
        ),
    )

    with pytest.raises(UserError, match="3754.*implicit type conversion failed.*'test_table'"):
        teradata_uploader.delete_by_record_id(file_data)


def test_teradata_uploader_delete_by_record_id_missing_table_raises_user_error(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """delete_by_record_id surfaces a 3807 as UserError, not a swallowed driver error."""
    # Skip the get_table_columns lookup that delete_by_record_id makes first.
    mocker.patch.object(
        teradata_uploader, "_get_db_column_name", return_value="record_id"
    )
    mock_cursor.execute.side_effect = _FakeTeradataDriverError(
        "[Error 3807] Object 'test_table' does not exist"
    )

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    with pytest.raises(UserError, match="3807.*'test_table'"):
        teradata_uploader.delete_by_record_id(file_data)


def test_teradata_uploader_upload_dataframe_no_privilege_raises_user_error(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """An INSERT that hits 3523 (no INSERT privilege) surfaces as UserError so the
    customer-visible message points at the privilege issue rather than retrying
    3 times into a generic 'Unexpected job failure'."""
    df = pd.DataFrame({"id": [1], "text": ["hi"], "record_id": ["f1"]})
    teradata_uploader._columns = ["id", "text", "record_id"]
    mocker.patch.object(teradata_uploader, "_fit_to_schema", return_value=df)
    mocker.patch.object(teradata_uploader, "can_delete", return_value=False)
    mock_cursor.executemany.side_effect = _FakeTeradataDriverError(
        "[Error 3523] User test_user does not have INSERT privilege on test_table"
    )

    file_data = FileData(
        identifier="test_file.txt",
        connector_type="local",
        source_identifiers=SourceIdentifiers(
            filename="test_file.txt", fullpath="/path/to/test_file.txt"
        ),
    )

    with pytest.raises(UserError, match="3523.*does not have the required privilege"):
        teradata_uploader.upload_dataframe(df, file_data)


def test_teradata_uploader_create_destination_no_privilege_raises_user_error(
    mock_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
    mock_get_cursor: MagicMock,
):
    """When a customer's user lacks CREATE TABLE privilege, auto-creation surfaces
    the privilege error as UserError instead of a bare driver exception.

    Note: this test deliberately creates an uploader with table_name=None so that
    create_destination()'s ``table_name = self.upload_config.table_name or
    destination_name`` resolves to ``destination_name`` (the production code path
    for opinionated writes). The shared fixture sets table_name='test_table' which
    would short-circuit destination_name."""
    uploader = TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(table_name=None, record_id_key="record_id"),
    )
    # cursor.execute side_effects, in call order:
    #   1. SELECT DATABASE             → None (sets up fetchone for current_db)
    #   2. SELECT 1 FROM DBC.TablesV   → None (table-exists check)
    #   3. CREATE MULTISET TABLE ...   → raises driver error
    # cursor.fetchone side_effects:
    #   1. ('test_db',)  → current_db
    #   2. None          → "table doesn't exist", proceed to CREATE
    mock_cursor.fetchone.side_effect = [("test_db",), None]
    mock_cursor.execute.side_effect = [
        None,
        None,
        _FakeTeradataDriverError(
            "[Error 3523] User test_user does not have CREATE TABLE privilege"
        ),
    ]

    with pytest.raises(
        UserError,
        match=r"3523.*does not have the required privilege.*brand_new_table",
    ):
        uploader.create_destination(destination_name="brand_new_table")


# --- Run-path auto-create (lazy create_destination in upload_dataframe) -------
# init() is the only auto-create trigger and only the local Pipeline calls it, so
# upload_dataframe must ensure the table exists itself — idempotently — or
# orchestrators that skip init() fail with Teradata 3807 ("object does not exist").

_OPINIONATED_COLUMNS = [
    ("id",), ("record_id",), ("element_id",), ("text",), ("type",), ("metadata",),
]


def _opinionated_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["1"],
            "record_id": ["r1"],
            "element_id": ["e1"],
            "text": ["hello"],
            "type": ["NarrativeText"],
            "metadata": ["{}"],
        }
    )


def _upload_file_data() -> FileData:
    return FileData(
        identifier="r1",
        connector_type="teradata",
        source_identifiers=SourceIdentifiers(filename="test.txt", fullpath="test.txt"),
    )


def test_teradata_uploader_upload_dataframe_auto_creates_named_table_when_missing(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    teradata_connection_config: TeradataConnectionConfig,
):
    """Reproduces the production failure (Pylon #2098): a destination connector with
    a table name given (e.g. 'uns_382') but the table not pre-created. The platform
    drives the upload run without calling init(), so upload_dataframe must auto-create
    the opinionated table instead of failing with Teradata 3807 'object does not exist'.
    """
    uploader = TeradataUploader(
        connection_config=teradata_connection_config,
        upload_config=TeradataUploaderConfig(table_name="uns_382", record_id_key="record_id"),
    )
    # create_destination: SELECT DATABASE -> current_db, DBC.TablesV -> None (missing) -> CREATE
    mock_cursor.fetchone.side_effect = [("test_db",), None]
    mock_cursor.description = _OPINIONATED_COLUMNS
    mock_cursor.rowcount = 0

    uploader.upload_dataframe(df=_opinionated_df(), file_data=_upload_file_data())

    assert uploader.upload_config.table_name == "uns_382"
    create_calls = [
        c[0][0] for c in mock_cursor.execute.call_args_list if "CREATE MULTISET TABLE" in c[0][0]
    ]
    assert len(create_calls) == 1
    assert '"uns_382"' in create_calls[0]
    insert_calls = [c[0][0] for c in mock_cursor.executemany.call_args_list]
    assert any('INSERT INTO "uns_382"' in c for c in insert_calls)


def test_teradata_uploader_upload_dataframe_auto_creates_default_table_when_name_is_none(
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """When no table name is configured, upload_dataframe auto-creates the default
    opinionated table rather than issuing SQL against a null table name."""
    assert teradata_uploader_auto_create.upload_config.table_name is None
    mock_cursor.fetchone.side_effect = [("test_db",), None]
    mock_cursor.description = _OPINIONATED_COLUMNS
    mock_cursor.rowcount = 0

    teradata_uploader_auto_create.upload_dataframe(
        df=_opinionated_df(), file_data=_upload_file_data()
    )

    assert teradata_uploader_auto_create.upload_config.table_name == DEFAULT_TABLE_NAME
    assert any(
        "CREATE MULTISET TABLE" in c[0][0] for c in mock_cursor.execute.call_args_list
    )
    assert any("INSERT INTO" in c[0][0] for c in mock_cursor.executemany.call_args_list)


def test_teradata_uploader_upload_dataframe_skips_create_when_table_exists(
    mock_cursor: MagicMock,
    teradata_uploader: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """upload_dataframe's auto-create is idempotent: when the destination table
    already exists, no CREATE is issued, so a user's pre-existing table is never
    clobbered. (create_destination's DBC.TablesV existence check guards this.)"""
    # create_destination: SELECT DATABASE -> current_db, DBC.TablesV -> (1,) exists -> no CREATE
    mock_cursor.fetchone.side_effect = [("test_db",), (1,)]
    mock_cursor.description = _OPINIONATED_COLUMNS
    mock_cursor.rowcount = 0

    teradata_uploader.upload_dataframe(df=_opinionated_df(), file_data=_upload_file_data())

    assert not any("CREATE" in c[0][0] for c in mock_cursor.execute.call_args_list)
    insert_calls = [c[0][0] for c in mock_cursor.executemany.call_args_list]
    assert any('INSERT INTO "test_table"' in c for c in insert_calls)


def test_teradata_uploader_init_marks_destination_ensured_so_upload_skips_reprobe(
    mocker: MockerFixture,
    mock_cursor: MagicMock,
    teradata_uploader_auto_create: TeradataUploader,
    mock_get_cursor: MagicMock,
):
    """When init() already ran create_destination (the local Pipeline path),
    upload_dataframe must not re-probe: create_destination is invoked exactly once."""
    mock_cursor.fetchone.side_effect = [("test_db",), None, ("test_db",), (1,)]
    mock_cursor.description = _OPINIONATED_COLUMNS
    mock_cursor.rowcount = 0
    spy = mocker.spy(teradata_uploader_auto_create, "create_destination")

    teradata_uploader_auto_create.init()
    assert teradata_uploader_auto_create._destination_ensured is True

    teradata_uploader_auto_create.upload_dataframe(
        df=_opinionated_df(), file_data=_upload_file_data()
    )

    assert spy.call_count == 1
