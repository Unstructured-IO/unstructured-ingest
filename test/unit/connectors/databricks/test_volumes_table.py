import json
from pathlib import Path

import pytest

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.databricks.volumes_table import (
    DatabricksVolumeDeltaTableStager,
    DatabricksVolumeDeltaTableStagerConfig,
    DatabricksVolumeDeltaTableUploaderConfig,
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


def test_stager_flatten_coerces_epoch_string_datetime(tmp_path: Path):
    """Stringified unix epochs in known datetime fields get coerced to ISO format so
    Databricks's implicit string -> TIMESTAMP cast succeeds (PLU-161 case I).

    Covers both top-level (`date_processed` injected by the pipeline at element root)
    and nested (`data_source_*`) datetime keys.
    """
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

    for key in (
        "date_processed",
        "data_source_date_created",
        "data_source_date_modified",
        "data_source_date_processed",
    ):
        assert isinstance(row[key], str)
        assert row[key].startswith("20"), f"{key} not in ISO format: {row[key]!r}"
        # confirm it round-trips through datetime.fromisoformat
        from datetime import datetime as _dt

        _dt.fromisoformat(row[key])


def test_stager_flatten_leaves_malformed_datetime_alone(tmp_path: Path):
    """Unparseable datetime values pass through unchanged so the destination table
    can reject them — matches the non-flatten path's behavior."""
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {"data_source": {"date_created": "not-a-date"}},
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=True)
    assert row["data_source_date_created"] == "not-a-date"


def test_stager_blob_mode_does_not_coerce_datetime(tmp_path: Path):
    """The non-flatten path is unchanged — datetime fields stay as strings inside
    the JSON blob, matching today's behavior."""
    elements = [
        {
            "element_id": "el-1",
            "text": "hello",
            "metadata": {"data_source": {"date_processed": "1779329564.5102773"}},
        }
    ]
    [row] = _run_stager(tmp_path, elements, flatten_metadata=False)
    assert "metadata" in row
    decoded = json.loads(row["metadata"])
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
