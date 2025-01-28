from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.v2.processes.connectors.databricks.volumes_table import (
    DatabricksVolumeDeltaTableStager,
)


@pytest.fixture
def stager():
    return DatabricksVolumeDeltaTableStager()


@pytest.mark.parametrize(
    ("output_path", "called_output_path"),
    [
        (
            Path("/fake/path/output"),
            Path("/fake/path/output.json"),
        ),
        (
            Path("/fake/path/output.ndjson"),
            Path("/fake/path/output.json"),
        ),
    ],
)
def test_write_output(
    mocker: MockerFixture,
    stager: DatabricksVolumeDeltaTableStager,
    output_path: Path,
    called_output_path: Path,
):
    data = [{"key1": "value1", "key2": "value2"}]

    mock_get_data = mocker.patch(
        "unstructured_ingest.v2.processes.connectors.databricks.volumes_table.write_data",
        return_value=None,
    )

    stager.write_output(output_path, data)

    mock_get_data.assert_called_once_with(path=called_output_path, data=data, indent=None)
