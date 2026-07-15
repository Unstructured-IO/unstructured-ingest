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


def _put_sql(mock_cursor: MagicMock) -> str:
    return next(sql for sql in _executed_sql(mock_cursor) if sql.startswith("PUT"))


def _sql_wellformed_problems(sql: str) -> list[str]:
    """Scan for premature literal/identifier termination, mirroring Databricks parsing:
    inside a literal a backslash escapes the next char; a bare ' always closes the literal
    ('' is NOT a doubled-quote escape below Spark master), while backticks are doubled."""
    i, n = 0, len(sql)
    in_str = in_ident = False
    while i < n:
        c = sql[i]
        if in_str:
            if c == "\\":  # backslash escapes the next char; skip both
                i += 2
                continue
            if c == "'":  # a bare quote closes the literal -- no '' escape in Databricks
                in_str = False
        elif in_ident:
            if c == "`":
                if i + 1 < n and sql[i + 1] == "`":  # doubled backtick stays in the identifier
                    i += 2
                    continue
                in_ident = False
        elif c == "'":
            in_str = True
        elif c == "`":
            in_ident = True
        i += 1
    problems = []
    if in_str:
        problems.append("unterminated single-quoted literal")
    if in_ident:
        problems.append("unterminated backtick identifier")
    return problems


def _decode_sql_tokens(sql: str) -> tuple[list[str], list[str]]:
    """Return (literals, identifiers): un-escaped contents of each single-quoted literal and
    backtick identifier, so the real path PUT and INSERT reference can be compared."""
    literals: list[str] = []
    identifiers: list[str] = []
    i, n = 0, len(sql)
    buf: list[str] = []
    in_str = in_ident = False
    while i < n:
        c = sql[i]
        if in_str:
            if c == "\\" and i + 1 < n:
                buf.append(sql[i + 1])
                i += 2
                continue
            if c == "'":  # a bare quote closes the literal (no '' escape in Databricks)
                literals.append("".join(buf))
                buf = []
                in_str = False
                i += 1
                continue
            buf.append(c)
        elif in_ident:
            if c == "`":
                if i + 1 < n and sql[i + 1] == "`":
                    buf.append("`")
                    i += 2
                    continue
                identifiers.append("".join(buf))
                buf = []
                in_ident = False
                i += 1
                continue
            buf.append(c)
        elif c == "'":
            in_str = True
        elif c == "`":
            in_ident = True
        i += 1
    return literals, identifiers


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


@pytest.mark.parametrize(
    "filename",
    ["owner's report.pdf", "quarter's summary.xlsm", "o'brien's file.pdf"],
)
def test_run_put_escapes_single_quotes_in_filename(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock, filename: str
):
    """Apostrophes in the filename must not break the single-quoted PUT literals
    (SQLSTATE 42601); Databricks escapes quotes with a backslash (\\'), NOT by doubling."""
    uploader = _make_uploader(flatten_metadata=True)
    uploader._columns = {"element_id": "string", "text": "string"}
    path = _staged_elements(tmp_path, {"element_id": "el-1", "text": "hello"})
    file_data = FileData(
        identifier="doc",
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(filename=filename, fullpath=filename),
    )

    uploader.run(path=path, file_data=file_data)

    put_sql = _put_sql(mock_cursor)
    # Backslash-escaped form present; the raw name is never a lone literal; and the
    # statement stays structurally valid (no literal terminated early).
    assert filename.replace("'", "\\'") in put_sql
    assert f"'{filename}'" not in put_sql
    assert _sql_wellformed_problems(put_sql) == []
    # The PUT target literal decodes back to the intended output path.
    expected = uploader.get_output_path(file_data=file_data)
    put_literals, _ = _decode_sql_tokens(put_sql)
    assert put_literals[1] == expected


def test_delete_previous_content_escapes_single_quotes_in_identifier(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """The DELETE runs before the PUT and quotes file_data.identifier; a path-derived
    identifier with an apostrophe must be escaped or the re-sync delete fails first."""
    uploader = _make_uploader(flatten_metadata=True)
    # record_id column present → can_delete() is True, so run() issues the DELETE first.
    uploader._columns = {"element_id": "string", "text": "string", "record_id": "string"}
    identifier = "folderA/owner's report.pdf"
    file_data = FileData(
        identifier=identifier,
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(filename="report.pdf", fullpath=identifier),
    )
    path = _staged_elements(tmp_path, {"element_id": "el-1", "text": "hello", "record_id": "x"})

    uploader.run(path=path, file_data=file_data)

    delete_sql = next(
        s for s in _executed_sql(mock_cursor) if s.strip().upper().startswith("DELETE")
    )
    escaped_identifier = identifier.replace("'", "\\'")
    assert f"record_id = '{escaped_identifier}'" in delete_sql
    # Raw apostrophe never sits as a lone literal boundary; the literal stays terminated
    # and decodes back to the exact identifier the row was stored under.
    assert f"'{identifier}'" not in delete_sql
    assert _sql_wellformed_problems(delete_sql) == []
    literals, _ = _decode_sql_tokens(delete_sql)
    assert literals == [identifier]


# Filenames exercising SQL metacharacters: apostrophe hits the single-quoted PUT/DELETE
# literals, backtick hits the backtick-quoted INSERT identifier; the rest guard regressions.
_SPECIAL_CHAR_FILENAMES = [
    "owner's report.pdf",  # single quote -> PUT/DELETE literals
    "o'brien's file.pdf",  # multiple single quotes
    "weird`name.pdf",  # backtick -> INSERT identifier
    'O\'Brien`s "weird" file;.pdf',  # quote + backtick + dquote + semicolon together
    'say "hi".pdf',  # double quotes
    "a;drop table x.pdf",  # semicolon (injection-shaped)
    "50%done_v_1.pdf",  # LIKE wildcards % and _
    "file[1](2){x}.pdf",  # brackets/braces
    "résumé_café.pdf",  # unicode
    "path\\to\\file.pdf",  # backslash
    "a\\'b.pdf",  # backslash immediately before a quote -> must not escape the quote
    "x\\' OR 1=1 --.pdf",  # injection-shaped: \\' + ' would break out if backslash unescaped
]


@pytest.mark.parametrize("filename", _SPECIAL_CHAR_FILENAMES)
def test_run_emits_wellformed_sql_for_special_char_filenames(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock, filename: str
):
    """Every statement run() emits (DELETE, PUT, INSERT) stays structurally valid for any
    metacharacter in the filename — no literal/identifier terminated early (SQLSTATE 42601)."""
    uploader = _make_uploader(flatten_metadata=True)
    uploader._columns = {"element_id": "string", "text": "string", "record_id": "string"}
    path = _staged_elements(tmp_path, {"element_id": "el-1", "text": "hello", "record_id": "r"})
    file_data = FileData(
        identifier=filename,
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(filename=filename, fullpath=filename),
    )

    uploader.run(path=path, file_data=file_data)

    executed = _executed_sql(mock_cursor)
    # Sanity: all three metacharacter-carrying statements were actually emitted.
    assert any(s.startswith("DELETE") for s in executed)
    assert any(s.startswith("PUT") for s in executed)
    assert any(s.startswith("INSERT") for s in executed)
    for sql in executed:
        problems = _sql_wellformed_problems(sql)
        assert not problems, f"{problems} in: {sql}"


@pytest.mark.parametrize("filename", _SPECIAL_CHAR_FILENAMES)
def test_put_target_and_insert_source_resolve_to_same_path(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock, filename: str
):
    """PUT's single-quoted target and INSERT's backtick source use different escaping;
    assert they decode to the same path, else INSERT reads the wrong file and loads nothing."""
    uploader = _make_uploader(flatten_metadata=True)
    uploader._columns = {"element_id": "string", "text": "string"}
    path = _staged_elements(tmp_path, {"element_id": "el-1", "text": "hello"})
    file_data = FileData(
        identifier="doc",
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(filename=filename, fullpath=filename),
    )

    uploader.run(path=path, file_data=file_data)

    expected = uploader.get_output_path(file_data=file_data)
    # PUT '<local>' INTO '<target>' OVERWRITE — the target is the 2nd single-quoted literal.
    put_literals, _ = _decode_sql_tokens(_put_sql(mock_cursor))
    put_target = put_literals[1]
    # INSERT ... FROM json.`<source>` — the source path is a backtick identifier.
    _, insert_idents = _decode_sql_tokens(_insert_sql(mock_cursor))
    insert_source = next(t for t in insert_idents if t == expected)

    assert put_target == expected
    assert insert_source == expected
    assert put_target == insert_source


@pytest.mark.parametrize(
    "sql",
    [
        "PUT '/vol/o\\'brien.json' INTO '/vol/o\\'brien.json' OVERWRITE",  # backslash-escaped '
        "SELECT * FROM `weird``name`",  # doubled backtick OK
        "WHERE x = 'back\\\\slash'",  # doubled backslash stays inside the literal
    ],
)
def test_oracle_accepts_wellformed_sql(sql: str):
    assert _sql_wellformed_problems(sql) == []


@pytest.mark.parametrize(
    "sql",
    [
        "WHERE x = 'unterminated",  # never closed
        "SELECT * FROM `unterminated",  # identifier never closed
        # Regression guard: a lone trailing backslash escapes the closing quote, so the
        # literal runs on -- this is why quote_literal MUST double source backslashes.
        "PUT 'ends\\' INTO 'x'",
    ],
)
def test_oracle_rejects_broken_or_injectable_sql(sql: str):
    assert _sql_wellformed_problems(sql) != []


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


def test_run_deletes_by_record_identifier_independent_of_volume_path(
    tmp_path: Path, mock_cursor: MagicMock, mock_get_cursor: MagicMock
):
    """Re-sync deletes key on file_data.identifier (the record_id column), not on the
    staging volume path. The relative-path change to get_output_path must not shift
    which rows a delete targets — otherwise rows could be orphaned on re-sync."""
    uploader = _make_uploader(flatten_metadata=True)
    # record_id column present → can_delete() is True, so run() deletes before insert.
    uploader._columns = {"element_id": "string", "text": "string", "record_id": "string"}
    file_data = FileData(
        identifier="record-identity-123",
        connector_type="databricks_volume_delta_tables",
        source_identifiers=SourceIdentifiers(
            filename="report.pdf",
            fullpath="folderA/report.pdf",
            rel_path="folderA/report.pdf",
        ),
    )
    path = _staged_elements(
        tmp_path, {"element_id": "el-1", "text": "hello", "record_id": "record-identity-123"}
    )

    uploader.run(path=path, file_data=file_data)

    delete_sql = next(
        s for s in _executed_sql(mock_cursor) if s.strip().upper().startswith("DELETE")
    )
    # Deletes by the deterministic identifier...
    assert "record_id = 'record-identity-123'" in delete_sql
    # ...and not by anything derived from the (now relative) volume staging path.
    assert "folderA" not in delete_sql
    assert "report.pdf" not in delete_sql
