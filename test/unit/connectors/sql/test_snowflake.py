import dataclasses
from unittest.mock import MagicMock

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.processes.connectors.sql.snowflake import (
    SnowflakeAccessConfig,
    SnowflakeConnectionConfig,
    SnowflakeUploader,
    SnowflakeUploaderConfig,
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
    # Deliberately does NOT hand-initialize _embeddings_dimension / _variant_columns.
    # It used to, and that redundant belt-and-braces turned load-bearing the moment the
    # two dataclass fields were dropped: 300 lines of tests stayed green while every
    # Snowflake upload raised AttributeError on its first batch. The fields' own defaults
    # are now what every test here runs against.
    return SnowflakeUploader(
        connection_config=snowflake_connection_config,
        upload_config=SnowflakeUploaderConfig(table_name="test_table"),
    )


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


# --- embeddings_dimension property ---


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


# --- _parse_select ---


def test_parse_select_with_vector_columns(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._embeddings_dimension = 128
    snowflake_uploader._variant_columns = []
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON($1)::VECTOR(FLOAT,128),PARSE_JSON($2),$3"
    assert result == expected_result


def test_parse_select_with_vector_columns_embedding_dimension_zero(
    snowflake_uploader: SnowflakeUploader,
):
    snowflake_uploader._embeddings_dimension = 0
    snowflake_uploader._variant_columns = []
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON($1),PARSE_JSON($2),$3"
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
    snowflake_uploader._variant_columns = []
    columns = ["embeddings", "languages", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    expected_result = "PARSE_JSON($1),PARSE_JSON($2),$3"
    assert result == expected_result


def test_parse_select_with_variant_columns(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._embeddings_dimension = 0
    snowflake_uploader._variant_columns = ["metadata"]
    columns = ["text", "metadata", "other_column"]

    result = snowflake_uploader._parse_select(columns)

    assert result == "$1,PARSE_JSON($2),$3"


def test_parse_select_variant_and_array_and_vector(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._embeddings_dimension = 64
    snowflake_uploader._variant_columns = ["metadata"]
    columns = ["embeddings", "languages", "metadata", "text"]

    result = snowflake_uploader._parse_select(columns)

    assert result == "PARSE_JSON($1)::VECTOR(FLOAT,64),PARSE_JSON($2),PARSE_JSON($3),$4"


# --- variant_columns property ---


def test_variant_columns_tuple_rows(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchall.return_value = [
        ("", "", "metadata", '{"type": "VARIANT"}', "", ""),
        ("", "", "Other_Col", '{"type": "TEXT"}', "", ""),
        ("", "", "extra_meta", '{"type": "VARIANT"}', "", ""),
    ]

    result = snowflake_uploader.variant_columns

    assert result == ["metadata", "extra_meta"]
    mock_cursor.execute.assert_called_once_with("SHOW COLUMNS IN test_table")


def test_variant_columns_dict_rows(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchall.return_value = [
        {"column_name": "Metadata", "data_type": '{"type": "VARIANT"}'},
        {"column_name": "text", "data_type": '{"type": "TEXT"}'},
    ]

    result = snowflake_uploader.variant_columns

    assert result == ["metadata"]


def test_variant_columns_empty(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchall.return_value = [
        ("", "", "col1", '{"type": "TEXT"}', "", ""),
        ("", "", "col2", '{"type": "NUMBER"}', "", ""),
    ]

    result = snowflake_uploader.variant_columns

    assert result == []


def test_variant_columns_cached(
    mock_cursor: MagicMock, snowflake_uploader: SnowflakeUploader, mock_get_cursor: MagicMock
):
    mock_cursor.execute.return_value.fetchall.return_value = [
        ("", "", "metadata", '{"type": "VARIANT"}', "", ""),
    ]

    snowflake_uploader.variant_columns
    snowflake_uploader.variant_columns

    # cursor should only be called once due to caching
    mock_cursor.execute.assert_called_once()


# --- prepare_data with variant columns ---


def test_prepare_data_variant_dict_value(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._variant_columns = ["metadata"]
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "metadata"]
    data = (("hello", {"key": "value"}),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", '{"key": "value"}')]


def test_prepare_data_variant_list_value(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._variant_columns = ["metadata"]
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "metadata"]
    data = (("hello", ["a", "b"]),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", '["a", "b"]')]


def test_prepare_data_variant_none_value(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._variant_columns = ["metadata"]
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "metadata"]
    data = (("hello", None),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", None)]


def test_prepare_data_variant_nan_value(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._variant_columns = ["metadata"]
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "metadata"]
    data = (("hello", float("nan")),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", None)]


def test_prepare_data_variant_string_passthrough(snowflake_uploader: SnowflakeUploader):
    """Non-null, non-list, non-dict values in variant columns pass through as-is."""
    snowflake_uploader._variant_columns = ["metadata"]
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "metadata"]
    data = (("hello", "raw_string"),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", "raw_string")]


def test_prepare_data_non_variant_column_passthrough(snowflake_uploader: SnowflakeUploader):
    snowflake_uploader._variant_columns = []
    snowflake_uploader._embeddings_dimension = 0
    columns = ["text", "num"]
    data = (("hello", 42),)

    result = snowflake_uploader.prepare_data(columns, data)

    assert result == [("hello", 42)]


class _FakeSnowflakeError(Exception):
    """Stand-in for snowflake.connector.errors.ProgrammingError.

    `snowflake-connector-python` is the `snowflake` extra, not part of the base `test`
    dependency group, so constructing a real driver error raises ModuleNotFoundError
    wherever the extra is not installed. The classifier reads exactly two things, both
    through `getattr`: `sqlstate` and `errno`. The defaults mirror the driver's own
    (`Error.__init__` fills errno=-1 and sqlstate="n/a" when the server sends neither),
    so an unset field means here what it means there. `test_the_driver_error_carries_the
    _two_fields_the_classifier_reads` pins that against the real driver.
    """

    def __init__(self, msg="SQL access control error", errno=-1, sqlstate="n/a"):
        super().__init__(msg)
        self.errno = errno
        self.sqlstate = sqlstate


def _snowflake_error(**kwargs):
    return _FakeSnowflakeError(**kwargs)


def test_the_driver_error_carries_the_two_fields_the_classifier_reads():
    """Everything above runs against the fake, so the fake's shape is asserted against the
    real driver wherever it is installed. If ProgrammingError stopped filling errno and
    sqlstate, or changed its unset defaults, the classifier would go blind in production
    while this file stayed green."""
    errors = pytest.importorskip(
        "snowflake.connector.errors", reason="snowflake-connector-python is the snowflake extra"
    )

    filled = errors.ProgrammingError(msg="SQL access control error", errno=3001, sqlstate="42501")
    unset = errors.ProgrammingError(msg="SQL access control error")

    assert (filled.errno, filled.sqlstate) == (3001, "42501")
    assert (unset.errno, unset.sqlstate) == (-1, "n/a")


@pytest.mark.parametrize("privilege", ["INSERT", "DELETE"])
def test_access_control_sqlstate_is_a_write_denial(
    snowflake_uploader: SnowflakeUploader, privilege: str
):
    # One access-control error covers every refused right, so the probe's privilege is
    # what names the missing grant.
    reason = snowflake_uploader.classify_write_denial(
        _snowflake_error(sqlstate="42501"), privilege=privilege
    )

    assert reason is not None
    assert f"{privilege} permission on table" in reason


def test_access_control_errno_is_a_write_denial(snowflake_uploader: SnowflakeUploader):
    reason = snowflake_uploader.classify_write_denial(
        _snowflake_error(errno=3001), privilege="DELETE"
    )

    assert reason is not None
    assert "DELETE permission on table" in reason


@pytest.mark.parametrize(
    "kwargs",
    [
        # "Object does not exist, or operation cannot be performed" — Snowflake uses one
        # wording for an absent name and for one the role may not see.
        {"errno": 2003, "sqlstate": "42S02"},
        {"errno": 1003, "sqlstate": "42000"},
        {"errno": 604},
        {},
    ],
)
def test_other_snowflake_errors_are_not_write_denials(
    snowflake_uploader: SnowflakeUploader, kwargs
):
    error = _snowflake_error(**kwargs)
    assert snowflake_uploader.classify_write_denial(error, privilege="INSERT") is None


def test_a_non_driver_exception_is_not_a_write_denial(snowflake_uploader: SnowflakeUploader):
    assert (
        snowflake_uploader.classify_write_denial(TimeoutError("no answer"), privilege="DELETE")
        is None
    )


def test_a_fresh_uploader_can_read_its_cached_properties(
    snowflake_connection_config: SnowflakeConnectionConfig,
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
):
    """Constructed the way the pipeline constructs it, with nothing assigned by hand.

    `variant_columns` and `embeddings_dimension` both read a cached attribute before they
    fill it. When the two dataclass fields backing them were dropped, a fresh uploader
    raised `AttributeError: 'SnowflakeUploader' object has no attribute
    '_embeddings_dimension'` on its first batch -- and the precheck, which never touches
    them, passed cleanly first. Nothing in this file caught it, because the fixture set
    both by hand.
    """
    uploader = SnowflakeUploader(
        connection_config=snowflake_connection_config,
        upload_config=SnowflakeUploaderConfig(table_name="test_table"),
    )
    mock_cursor.execute.return_value.fetchall.return_value = []
    mock_cursor.execute.return_value.fetchone.return_value = None

    assert uploader.variant_columns == []
    assert uploader.embeddings_dimension == 0


def test_the_cached_fields_are_declared_on_the_dataclass(
    snowflake_uploader: SnowflakeUploader,
):
    """The narrower guard on the same regression: the two fields are declared, so no
    ordering of property reads can find them undefined."""
    declared = {field.name for field in dataclasses.fields(snowflake_uploader)}

    assert {"_embeddings_dimension", "_variant_columns"} <= declared
    assert snowflake_uploader._embeddings_dimension is None
    assert snowflake_uploader._variant_columns is None
