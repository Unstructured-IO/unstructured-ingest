from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.data_types.file_data import BatchItem, FileData, SourceIdentifiers
from unstructured_ingest.interfaces import DownloadResponse
from unstructured_ingest.processes.connectors.sql.sql import (
    SqlAdditionalMetadata,
    SqlBatchFileData,
    SQLConnectionConfig,
    SQLDownloader,
    SQLDownloaderConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
)


@pytest.fixture
def mock_instance() -> SQLUploadStager:
    return SQLUploadStager()


@pytest.fixture
def mock_uploader(mocker: MockerFixture) -> SQLUploader:
    mock_connection_config = mocker.Mock(spec=SQLConnectionConfig)
    mock_upload_config = mocker.Mock(spec=SQLUploaderConfig)
    return SQLUploader(
        upload_config=mock_upload_config,
        connection_config=mock_connection_config,
        connector_type="sql_test",
    )


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
    mock_instance: SQLUploadStager,
    input_filepath: str,
    output_filename: str,
    expected: str,
):
    output_dir = Path("/tmp/test/output_dir")

    # Mocks
    mock_get_data = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.sql.get_json_data",
        return_value=[{"key": "value"}, {"key": "value2"}],
    )
    mock_conform_dict = mocker.patch.object(
        SQLUploadStager, "conform_dict", side_effect=lambda element_dict, file_data: element_dict
    )
    mock_conform_dataframe = mocker.patch.object(
        SQLUploadStager, "conform_dataframe", side_effect=lambda df: df
    )
    mock_get_output_path = mocker.patch.object(
        SQLUploadStager, "get_output_path", return_value=output_dir / expected
    )
    mock_write_output = mocker.patch(
        "unstructured_ingest.processes.connectors.sql.sql.write_data", return_value=None
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
    mock_conform_dataframe.assert_called_once()
    mock_get_output_path.assert_called_once_with(output_filename=expected, output_dir=output_dir)
    mock_write_output.assert_called_once_with(
        path=output_dir / expected, data=[{"key": "value"}, {"key": "value2"}]
    )
    assert result.name == expected


def test_fit_to_schema_drop_columns(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [3, 4],
            "col3": [5, 6],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["col1", "col2"])

    result = mock_uploader._fit_to_schema(df)

    assert "col3" not in result.columns
    assert "col1" in result.columns
    assert "col2" in result.columns


def test_fit_to_schema_add_missing_columns(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["col1", "col2"])

    result = mock_uploader._fit_to_schema(df)

    assert "col2" in result.columns
    assert result["col2"].isnull().all()


def test_fit_to_schema_no_changes(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [3, 4],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["col1", "col2"])

    result = mock_uploader._fit_to_schema(df)

    assert "col1" in result.columns
    assert "col2" in result.columns
    assert result.equals(df)


def test_fit_to_schema_no_add_missing_columns(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["col1", "col2"])

    result = mock_uploader._fit_to_schema(df, add_missing_columns=False)

    assert "col2" not in result.columns
    assert "col1" in result.columns


def test_fit_to_schema_case_sensitive(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [3, 4],
            "col3": [5, 6],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["COL1", "COL2", "col3"])

    result = mock_uploader._fit_to_schema(df)

    assert "col1" not in result.columns
    assert "col2" not in result.columns
    assert "col3" in result.columns
    assert "COL1" in result.columns
    assert "COL2" in result.columns


def test_fit_to_schema_not_case_sensitive(mocker: MockerFixture, mock_uploader: SQLUploader):
    df = pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [3, 4],
            "col3": [5, 6],
        }
    )
    mocker.patch.object(mock_uploader, "get_table_columns", return_value=["COL1", "COL2"])

    result = mock_uploader._fit_to_schema(df, add_missing_columns=False, case_sensitive=False)

    assert "col3" not in result.columns
    assert "col1" in result.columns
    assert "col2" in result.columns


class TestCanDelete:
    def test_exact_match(self, mocker: MockerFixture, mock_uploader: SQLUploader):
        mocker.patch.object(mock_uploader, "get_table_columns", return_value=["record_id", "text"])
        mock_uploader.upload_config.record_id_key = "record_id"
        assert mock_uploader.can_delete() is True

    def test_case_insensitive_match(self, mocker: MockerFixture, mock_uploader: SQLUploader):
        mocker.patch.object(
            mock_uploader, "get_table_columns", return_value=["RECORD_ID", "TEXT"]
        )
        mock_uploader.upload_config.record_id_key = "record_id"
        assert mock_uploader.can_delete() is True

    def test_no_match(self, mocker: MockerFixture, mock_uploader: SQLUploader):
        mocker.patch.object(mock_uploader, "get_table_columns", return_value=["id", "text"])
        mock_uploader.upload_config.record_id_key = "record_id"
        assert mock_uploader.can_delete() is False


class TestResolveColumnName:
    def test_exact_match(self):
        df = pd.DataFrame({"id": [1], "text": ["hello"]})
        assert SQLDownloader._resolve_column_name(df, "id") == "id"

    def test_case_insensitive_fallback(self):
        df = pd.DataFrame({"ID": [1], "TEXT": ["hello"]})
        assert SQLDownloader._resolve_column_name(df, "id") == "ID"

    def test_mixed_case_fallback(self):
        df = pd.DataFrame({"Id": [1], "Text": ["hello"]})
        assert SQLDownloader._resolve_column_name(df, "id") == "Id"

    def test_no_match_returns_original(self):
        df = pd.DataFrame({"foo": [1], "bar": ["hello"]})
        assert SQLDownloader._resolve_column_name(df, "id") == "id"

    def test_exact_match_preferred_over_case_insensitive(self):
        df = pd.DataFrame({"id": [1], "ID": [2]})
        assert SQLDownloader._resolve_column_name(df, "id") == "id"


class TestGenerateDownloadResponse:
    def _make_file_data(self, table_name="test_table", id_column="id"):
        return SqlBatchFileData(
            identifier="test",
            connector_type="test",
            source_identifiers=SourceIdentifiers(
                filename="test.csv", fullpath="test.csv"
            ),
            additional_metadata=SqlAdditionalMetadata(
                table_name=table_name, id_column=id_column
            ),
            batch_items=[BatchItem(identifier="1")],
        )

    def _make_downloader(self, tmp_path, mocker):
        downloader = mocker.MagicMock(spec=SQLDownloader)
        downloader.download_dir = tmp_path
        downloader.download_config = SQLDownloaderConfig(fields=["text"])
        downloader._resolve_column_name = SQLDownloader._resolve_column_name
        downloader.get_identifier = SQLDownloader.get_identifier.__get__(downloader)
        downloader.generate_download_response = (
            SQLDownloader.generate_download_response.__get__(downloader, type(downloader))
        )
        return downloader

    def test_matching_case(self, tmp_path, mocker):
        """generate_download_response works when column case matches id_column."""
        downloader = self._make_downloader(tmp_path, mocker)
        file_data = self._make_file_data(id_column="id")
        result_df = pd.DataFrame({"id": [42], "text": ["hello"]})

        mock_response = MagicMock(spec=DownloadResponse)
        mocker.patch(
            "unstructured_ingest.interfaces.Downloader.generate_download_response",
            return_value=mock_response,
        )

        response = downloader.generate_download_response(result=result_df, file_data=file_data)
        assert response == mock_response

    def test_uppercase_columns_lowercase_id_column(self, tmp_path, mocker):
        """generate_download_response resolves uppercase column to lowercase id_column."""
        downloader = self._make_downloader(tmp_path, mocker)
        file_data = self._make_file_data(id_column="id")
        result_df = pd.DataFrame({"ID": [42], "TEXT": ["hello"]})

        mock_response = MagicMock(spec=DownloadResponse)
        mocker.patch(
            "unstructured_ingest.interfaces.Downloader.generate_download_response",
            return_value=mock_response,
        )

        response = downloader.generate_download_response(result=result_df, file_data=file_data)
        assert response == mock_response
