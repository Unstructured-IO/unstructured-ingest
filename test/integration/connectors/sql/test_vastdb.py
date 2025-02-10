from pathlib import Path

import pytest
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from unstructured_ingest.v2.processes.connectors.sql.vastdb import (
    CONNECTOR_TYPE,
    VastdbUploadStager,
    VastdbUploadStagerConfig,
)


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def test_vast_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = VastdbUploadStager(
        upload_stager_config=VastdbUploadStagerConfig(rename_columns_map={"page_number": "page"})
    )
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
