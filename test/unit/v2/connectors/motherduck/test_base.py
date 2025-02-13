from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.v2.interfaces import FileData
from unstructured_ingest.v2.interfaces.file_data import SourceIdentifiers
from unstructured_ingest.v2.interfaces.upload_stager import UploadStagerConfig
from unstructured_ingest.v2.processes.connectors.duckdb.base import BaseDuckDBUploadStager


@pytest.fixture
def mock_instance() -> BaseDuckDBUploadStager:
    return BaseDuckDBUploadStager(UploadStagerConfig())


@pytest.mark.parametrize(
    ("input_filepath", "output_filename", "expected"),
    [
        (
            "/path/to/input_file.ndjson",
            "output_file.ndjson",
            "output_file.ndjson",
        ),
        ("input_file.txt", "output_file.json", "output_file.txt"),
        ("/path/to/input_file.json", "output_file", "output_file.json"),
    ],
)
def test_run_output_filename_suffix(
    mocker: MockerFixture,
    mock_instance: BaseDuckDBUploadStager,
    input_filepath: str,
    output_filename: str,
    expected: str,
):
    output_dir = Path("/tmp/test/output_dir")

    # Mocks
    mock_get_data = mocker.patch(
        "unstructured_ingest.v2.processes.connectors.duckdb.base.get_data",
        return_value=[{"key": "value"}, {"key": "value2"}],
    )
    mock_conform_dict = mocker.patch.object(
        BaseDuckDBUploadStager,
        "conform_dict",
        side_effect=lambda element_dict, file_data: element_dict,
    )
    mock_get_output_path = mocker.patch.object(
        BaseDuckDBUploadStager, "get_output_path", return_value=output_dir / expected
    )
    mock_write_output = mocker.patch(
        "unstructured_ingest.v2.processes.connectors.duckdb.base.write_data", return_value=None
    )

    # Act
    result = mock_instance.run(
        elements_filepath=Path(input_filepath),
        file_data=FileData(
            identifier="test",
            connector_type="test",
            source_identifiers=SourceIdentifiers(filename=input_filepath, fullpath=input_filepath),
        ),
        output_dir=output_dir,
        output_filename=output_filename,
    )

    # Assert
    mock_get_data.assert_called_once_with(path=Path(input_filepath))
    assert mock_conform_dict.call_count == 2
    mock_get_output_path.assert_called_once_with(output_filename=expected, output_dir=output_dir)
    mock_write_output.assert_called_once_with(
        path=output_dir / expected, data=[{"key": "value"}, {"key": "value2"}]
    )
    assert result.name == expected
