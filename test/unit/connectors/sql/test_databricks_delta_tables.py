from unittest.mock import MagicMock

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.sql.databricks_delta_tables import (
    DatabricksDeltaTablesAccessConfig,
    DatabricksDeltaTablesConnectionConfig,
    DatabricksDeltaTablesUploader,
    DatabricksDeltaTablesUploaderConfig,
)
from unstructured_ingest.utils.databricks import quote_identifier

HYPHENATED_CATALOG = "utic-dev-tech-fixtures"
HYPHENATED_DATABASE = "my-db"
TABLE_NAME = "elements"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("default", "`default`"),
        ("utic-dev-tech-fixtures", "`utic-dev-tech-fixtures`"),
        ("select", "`select`"),
        ("table", "`table`"),
        ("from", "`from`"),
        ("1name", "`1name`"),
        ("my schema", "`my schema`"),
        ("café", "`café`"),
        ("foo`bar", "`foo``bar`"),
        ("a``b", "`a````b`"),
        ("`", "````"),
        ("`leading", "```leading`"),
        ("trailing`", "`trailing```"),
    ],
)
def test_quote_identifier_happy_and_escaping(raw, expected):
    assert quote_identifier(raw) == expected


@pytest.mark.parametrize(
    "bad",
    [None, "", " ", "  ", "\t", "\n", "\t \n"],
)
def test_quote_identifier_rejects_empty_and_whitespace(bad):
    with pytest.raises(IngestValueError):
        quote_identifier(bad)


def test_quote_identifier_never_uses_single_quotes():
    out = quote_identifier("utic-dev-tech-fixtures")
    assert out.startswith("`") and out.endswith("`")
    assert "'" not in out


@pytest.fixture
def databricks_uploader():
    return DatabricksDeltaTablesUploader(
        connection_config=DatabricksDeltaTablesConnectionConfig(
            access_config=Secret(DatabricksDeltaTablesAccessConfig(token="tok")),
            server_hostname="example.databricks.com",
            http_path="/sql/1.0/warehouses/xxx",
        ),
        upload_config=DatabricksDeltaTablesUploaderConfig(
            catalog=HYPHENATED_CATALOG,
            database=HYPHENATED_DATABASE,
            table_name=TABLE_NAME,
        ),
    )


@pytest.fixture
def mock_cursor(mocker: MockerFixture):
    return mocker.MagicMock()


@pytest.fixture
def mock_get_cursor(mocker: MockerFixture, mock_cursor: MagicMock):
    mock = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.databricks_delta_tables"
        ".DatabricksDeltaTablesConnectionConfig.get_cursor",
        autospec=True,
    )
    mock.return_value.__enter__.return_value = mock_cursor
    return mock


def test_get_cursor_quotes_catalog_and_database(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    databricks_uploader: DatabricksDeltaTablesUploader,
):
    """Test that get_cursor emits backtick-quoted USE CATALOG/USE DATABASE (PLU-332)."""
    with databricks_uploader.get_cursor():
        pass

    executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
    assert f"USE CATALOG `{HYPHENATED_CATALOG}`" in executed
    assert f"USE DATABASE `{HYPHENATED_DATABASE}`" in executed
    for sql in executed:
        if sql.startswith("USE "):
            assert "'" not in sql


def test_precheck_quotes_catalog_and_database(
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    databricks_uploader: DatabricksDeltaTablesUploader,
):
    """Test that precheck emits backtick-quoted USE CATALOG/USE DATABASE (PLU-332)."""
    mock_cursor.fetchall.side_effect = [
        [(HYPHENATED_CATALOG,)],
        [(HYPHENATED_DATABASE,)],
        [("col1", TABLE_NAME)],
    ]

    databricks_uploader.precheck()

    executed = [c.args[0] for c in mock_cursor.execute.call_args_list]
    assert f"USE CATALOG `{HYPHENATED_CATALOG}`" in executed
    assert f"USE DATABASE `{HYPHENATED_DATABASE}`" in executed
    for sql in executed:
        if sql.startswith("USE "):
            assert "'" not in sql
