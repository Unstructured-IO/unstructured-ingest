import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import Secret
from pytest_mock import MockerFixture

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.databricks.volumes_table import (
    DatabricksVolumeDeltaTableStager,
    DatabricksVolumeDeltaTableStagerConfig,
    DatabricksVolumeDeltaTableUploader,
    DatabricksVolumeDeltaTableUploaderConfig,
)
from unstructured_ingest.processes.connectors.sql.databricks_delta_tables import (
    DatabricksDeltaTablesAccessConfig,
    DatabricksDeltaTablesConnectionConfig,
)


def _file_data() -> FileData:
    return FileData(
        identifier="doc-1",
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(
            filename="example.pdf",
            fullpath="s3://bucket/example.pdf",
        ),
    )


def _write_elements(path: Path, elements: list[dict]) -> Path:
    path.write_text(json.dumps(elements))
    return path


def _run_stager(tmp_path: Path, elements: list[dict], flatten_metadata: bool) -> list[dict]:
    elements_in = _write_elements(tmp_path / "elements.json", elements)
    stager = DatabricksVolumeDeltaTableStager(
        upload_stager_config=DatabricksVolumeDeltaTableStagerConfig(
            flatten_metadata=flatten_metadata
        )
    )
    out_path = stager.run(
        elements_filepath=elements_in,
        output_dir=tmp_path / "out",
        output_filename="elements.json",
        file_data=_file_data(),
    )
    return json.loads(Path(out_path).read_text())


def _baseline_metadata() -> dict:
    return {
        "filename": "example.pdf",
        "filetype": "application/pdf",
        "page_number": 1,
        "languages": ["eng"],
        "data_source": {
            "url": "s3://bucket/example.pdf",
            "version": "abc123",
            "record_locator": {"protocol": "s3", "remote_file_path": "s3://bucket/"},
        },
    }


def test_stager_blob_mode_is_default(tmp_path: Path):
    elements = [{"element_id": "el-1", "text": "hello", "metadata": _baseline_metadata()}]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=False)

    assert "metadata" in row
    assert isinstance(row["metadata"], str)
    assert json.loads(row["metadata"]) == _baseline_metadata()


def test_stager_flatten_drops_metadata_prefix(tmp_path: Path):
    elements = [{"element_id": "el-1", "text": "hello", "metadata": _baseline_metadata()}]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=True)

    assert "metadata" not in row
    assert row["filename"] == "example.pdf"
    assert row["filetype"] == "application/pdf"
    assert row["page_number"] == 1
    assert row["data_source_url"] == "s3://bucket/example.pdf"
    assert row["data_source_version"] == "abc123"
    assert row["data_source_record_locator_protocol"] == "s3"
    assert row["data_source_record_locator_remote_file_path"] == "s3://bucket/"
    assert not any(k.startswith("metadata_") for k in row)


def test_stager_flatten_stops_at_lists(tmp_path: Path):
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {
                "languages": ["eng", "fra"],
                "sent_to": ["a@example.com", "b@example.com"],
            },
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=True)

    assert row["languages"] == ["eng", "fra"]
    assert row["sent_to"] == ["a@example.com", "b@example.com"]
    assert "languages_0" not in row
    assert "sent_to_0" not in row


def test_stager_flatten_passes_datetime_strings_through_unchanged(tmp_path: Path):
    """Datetime fields flow into the flattened row as their JSON-native string form —
    no ISO coercion. Customers declare these columns as STRING in their Delta table."""
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {
                "date_processed": "1779329600.0",
                "data_source": {
                    "date_created": "1779329000.0",
                    "date_modified": "1779329500.0",
                    "date_processed": "1779329564.5102773",
                },
            },
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=True)

    assert row["date_processed"] == "1779329600.0"
    assert row["data_source_date_created"] == "1779329000.0"
    assert row["data_source_date_modified"] == "1779329500.0"
    assert row["data_source_date_processed"] == "1779329564.5102773"


def test_stager_flatten_passes_float_epoch_through_unchanged(tmp_path: Path):
    """JSON-parsed numeric epochs arrive as Python floats and must pass through
    untouched — neither converted to ISO strings nor stringified."""
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {"data_source": {"date_processed": 1779329564.5102773}},
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=True)

    assert row["data_source_date_processed"] == 1779329564.5102773
    assert isinstance(row["data_source_date_processed"], float)


def test_stager_blob_mode_preserves_datetime_strings(tmp_path: Path):
    """The non-flatten path is unchanged — datetime fields stay as strings inside
    the JSON blob with byte-identical values."""
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {
                "date_processed": "1779329600.0",
                "data_source": {"date_processed": "1779329564.5102773"},
            },
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=False)

    decoded = json.loads(row["metadata"])
    assert decoded["date_processed"] == "1779329600.0"
    assert decoded["data_source"]["date_processed"] == "1779329564.5102773"


@pytest.mark.parametrize(
    "config_cls",
    [DatabricksVolumeDeltaTableUploaderConfig, DatabricksVolumeDeltaTableStagerConfig],
)
def test_flatten_metadata_defaults_false_for_workflow_db_backcompat(config_cls):
    """Old configs persisted before PLU-161 have no `flatten_metadata` field. Deserialization
    must produce flatten_metadata=False so existing connectors are byte-identical."""
    kwargs = {"catalog": "c", "volume": "v"} if "Uploader" in config_cls.__name__ else {}
    config = config_cls.model_validate(kwargs)
    assert config.flatten_metadata is False


def _make_uploader(
    flatten_metadata: bool, table_name: str = "elements"
) -> DatabricksVolumeDeltaTableUploader:
    return DatabricksVolumeDeltaTableUploader(
        connection_config=DatabricksDeltaTablesConnectionConfig(
            access_config=Secret(DatabricksDeltaTablesAccessConfig(token="tok")),
            server_hostname="example.databricks.com",
            http_path="/sql/1.0/warehouses/xxx",
        ),
        upload_config=DatabricksVolumeDeltaTableUploaderConfig(
            catalog="cat",
            databricks_schema="sch",
            volume="vol",
            database="db",
            table_name=table_name,
            flatten_metadata=flatten_metadata,
        ),
    )


@pytest.fixture
def mock_cursor(mocker: MockerFixture) -> MagicMock:
    return mocker.MagicMock()


@pytest.fixture
def mock_get_cursor(mocker: MockerFixture, mock_cursor: MagicMock) -> MagicMock:
    mock = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.databricks_delta_tables"
        ".DatabricksDeltaTablesConnectionConfig.get_cursor",
        autospec=True,
    )
    mock.return_value.__enter__.return_value = mock_cursor
    return mock


def _executed_sql(mock_cursor: MagicMock) -> list[str]:
    return [c.args[0] for c in mock_cursor.execute.call_args_list]


def _insert_sql(mock_cursor: MagicMock) -> str:
    return next(sql for sql in _executed_sql(mock_cursor) if sql.startswith("INSERT"))


def test_create_destination_flatten_true_is_noop(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """Under flatten_metadata=true the user manages the destination table; precheck
    validates existence, so create_destination is a no-op and must not touch the
    warehouse (no SHOW TABLES, no CREATE TABLE)."""
    uploader = _make_uploader(flatten_metadata=True, table_name="missing_table")

    assert uploader.create_destination() is False

    assert _executed_sql(mock_cursor) == []


def test_create_destination_flatten_false_missing_table_autocreates(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """flatten=false still auto-creates from the asset schema when the destination
    table is missing."""
    uploader = _make_uploader(flatten_metadata=False, table_name="new_table")
    mock_cursor.fetchall.return_value = []  # SHOW TABLES → no rows

    assert uploader.create_destination() is True

    create_sqls = [sql for sql in _executed_sql(mock_cursor) if "CREATE TABLE" in sql]
    assert len(create_sqls) == 1
    assert "new_table" in create_sqls[0]


def test_create_destination_flatten_false_existing_table_returns_false(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """flatten=false with an existing table is a no-op — SHOW TABLES finds the
    name and we return False without issuing a CREATE."""
    uploader = _make_uploader(flatten_metadata=False, table_name="elements")
    # `SHOW TABLES` rows are `(database, tableName, isTemporary)`; r[1] is the name.
    mock_cursor.fetchall.return_value = [("sch", "elements", False)]

    assert uploader.create_destination() is False
    assert not any("CREATE TABLE" in sql for sql in _executed_sql(mock_cursor))


def _precheck_fetchall(*, catalogs, databases, tables):
    """SHOW CATALOGS / SHOW DATABASES / SHOW TABLES are the only fetchall calls in
    precheck, in that order. SHOW TABLES rows are (database, tableName, isTemporary)."""
    return [
        [(c,) for c in catalogs],
        [(d,) for d in databases],
        [("db", t, False) for t in tables],
    ]


def test_precheck_flatten_true_missing_table_raises(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """With flatten_metadata=true, precheck must fail fast when the destination table
    is missing — otherwise the per-document INSERT is the first thing that notices."""
    uploader = _make_uploader(flatten_metadata=True, table_name="missing_table")
    mock_cursor.fetchall.side_effect = _precheck_fetchall(
        catalogs=["cat"], databases=["db"], tables=["other_table"]
    )

    with pytest.raises(IngestValueError, match="must be pre-created"):
        uploader.precheck()


def test_precheck_flatten_true_existing_table_passes(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    uploader = _make_uploader(flatten_metadata=True, table_name="elements")
    mock_cursor.fetchall.side_effect = _precheck_fetchall(
        catalogs=["cat"], databases=["db"], tables=["elements"]
    )

    uploader.precheck()


def test_precheck_flatten_false_skips_table_check(
    mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """Backcompat: flatten=false leaves table existence to create_destination /
    auto-create, so precheck must not fail when the table is missing."""
    uploader = _make_uploader(flatten_metadata=False, table_name="missing_table")
    mock_cursor.fetchall.side_effect = [
        [("cat",)],  # SHOW CATALOGS
        [("db",)],  # SHOW DATABASES
    ]

    uploader.precheck()

    assert not any(sql == "SHOW TABLES" for sql in _executed_sql(mock_cursor))


def _staged_elements(tmp_path: Path, row: dict) -> Path:
    path = tmp_path / "staged.json"
    path.write_text(json.dumps([row]))
    return path


def test_run_flatten_true_select_uses_raw_columns(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """Under flatten_metadata=true the SELECT clause references columns directly —
    no PARSE_JSON wrapping, and no `metadata` column."""
    uploader = _make_uploader(flatten_metadata=True)
    # Pre-populate the columns cache: same keys as incoming, no record_id → can_delete=false
    uploader._columns = {
        "element_id": "string",
        "text": "string",
        "filename": "string",
        "data_source_url": "string",
    }
    path = _staged_elements(
        tmp_path,
        {
            "element_id": "el-1",
            "text": "hello",
            "filename": "x.pdf",
            "data_source_url": "s3://bucket/x.pdf",
        },
    )

    uploader.run(path=path, file_data=_file_data())

    insert_sql = _insert_sql(mock_cursor)
    assert "PARSE_JSON" not in insert_sql
    assert "metadata" not in insert_sql
    for col in ("element_id", "text", "filename", "data_source_url"):
        assert col in insert_sql


def test_run_flatten_false_select_wraps_metadata_in_parse_json(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """Regression guard for the today-default path: flatten_metadata=false must
    still wrap the `metadata` column with PARSE_JSON so the VARIANT cast works."""
    uploader = _make_uploader(flatten_metadata=False)
    uploader._columns = {"element_id": "string", "text": "string", "metadata": "variant"}
    path = _staged_elements(
        tmp_path,
        {"element_id": "el-1", "text": "hello", "metadata": json.dumps({"filename": "x.pdf"})},
    )

    uploader.run(path=path, file_data=_file_data())

    insert_sql = _insert_sql(mock_cursor)
    assert "PARSE_JSON(metadata)" in insert_sql


def test_run_flatten_true_drops_unknown_columns_and_logs(
    tmp_path: Path,
    mock_cursor: MagicMock,
    mock_get_cursor: MagicMock,
    caplog: pytest.LogCaptureFixture,
):
    """Under flatten_metadata=true, incoming columns not present in the destination
    table are dropped from the INSERT and surfaced in a single info log line."""
    uploader = _make_uploader(flatten_metadata=True)
    uploader._columns = {"element_id": "string", "text": "string", "filename": "string"}
    path = _staged_elements(
        tmp_path,
        {
            "element_id": "el-1",
            "text": "hello",
            "filename": "x.pdf",
            "unknown_col": "drop me",
            "another_extra": 42,
        },
    )

    with caplog.at_level("INFO", logger="unstructured_ingest"):
        uploader.run(path=path, file_data=_file_data())

    insert_sql = _insert_sql(mock_cursor)
    assert "unknown_col" not in insert_sql
    assert "another_extra" not in insert_sql
    drop_lines = [r for r in caplog.records if "dropped" in r.message.lower()]
    assert len(drop_lines) == 1
    assert "unknown_col" in drop_lines[0].message
    assert "another_extra" in drop_lines[0].message


def _file_data_rel(relative_path: str, filename: str) -> FileData:
    return FileData(
        identifier=relative_path,
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(
            filename=filename,
            fullpath=relative_path,
            rel_path=relative_path,
        ),
    )


def test_get_output_path_same_basename_different_folders_do_not_collide():
    """Two source files that share a basename but live in different folders must map
    to distinct volume paths — otherwise their staged files overwrite each other and
    the per-file table load races on a single shared path."""
    uploader = _make_uploader(flatten_metadata=True)

    path_a = uploader.get_output_path(file_data=_file_data_rel("folderA/report.pdf", "report.pdf"))
    path_b = uploader.get_output_path(file_data=_file_data_rel("folderB/report.pdf", "report.pdf"))

    assert path_a != path_b
    assert path_a == "/Volumes/cat/default/vol/folderA/report.pdf.json"
    assert path_b == "/Volumes/cat/default/vol/folderB/report.pdf.json"


def test_get_output_path_prefers_relative_path_over_filename():
    uploader = _make_uploader(flatten_metadata=True)

    path = uploader.get_output_path(file_data=_file_data_rel("nested/dir/report.pdf", "report.pdf"))

    assert path == "/Volumes/cat/default/vol/nested/dir/report.pdf.json"


def test_get_output_path_falls_back_to_filename_without_relative_path():
    uploader = _make_uploader(flatten_metadata=True)
    file_data = FileData(
        identifier="doc",
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(filename="report.pdf", fullpath=""),
    )

    path = uploader.get_output_path(file_data=file_data)

    assert path == "/Volumes/cat/default/vol/report.pdf.json"


def test_get_output_path_does_not_double_json_suffix():
    """A source file already ending in .json keeps a single suffix."""
    uploader = _make_uploader(flatten_metadata=True)

    path = uploader.get_output_path(
        file_data=_file_data_rel("folderA/already.json", "already.json")
    )

    assert path == "/Volumes/cat/default/vol/folderA/already.json"
