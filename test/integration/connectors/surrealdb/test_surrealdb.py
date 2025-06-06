import json
from pathlib import Path
import logging

import surrealdb
import pytest
from _pytest.fixtures import TopRequest

from test.integration.connectors.utils.constants import DESTINATION_TAG, SQL_TAG
from test.integration.connectors.utils.validation.destination import (
    StagerValidationConfigs,
    stager_validation,
)
from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.processes.connectors.surrealdb.surrealdb import (
    CONNECTOR_TYPE,
    SurrealDBConnectionConfig,
    SurrealDBUploader,
    SurrealDBUploaderConfig,
    SurrealDBUploadStager,
)

logger = logging.getLogger("surrealdb_test")


def validate_surrealdb_destination(config: SurrealDBConnectionConfig, expected_num_elements: int):
    with config.get_client() as client:
        _results = client.query("select count() from elements group all;")
        logger.debug(f"results: {_results}")
        _count = _results[0]["count"]
    assert _count == expected_num_elements, (
        f"dest check failed: got {_count}, expected {expected_num_elements}"
    )


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "surrealdb", SQL_TAG)
def test_surrealdb_destination(
    upload_file: Path, config: SurrealDBConnectionConfig, temp_dir: Path
):
    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=CONNECTOR_TYPE,
        identifier="mock-file-data",
    )

    stager = SurrealDBUploadStager()
    staged_path = stager.run(
        elements_filepath=upload_file,
        file_data=file_data,
        output_dir=temp_dir,
        output_filename=upload_file.name,
    )

    upload_config = SurrealDBUploaderConfig()
    uploader = SurrealDBUploader(connection_config=config, upload_config=upload_config)

    uploader.run(path=staged_path, file_data=file_data)

    with staged_path.open() as f:
        data = json.load(f)
    validate_surrealdb_destination(config=config, expected_num_elements=len(data))


@pytest.mark.tags(CONNECTOR_TYPE, DESTINATION_TAG, "surrealdb", SQL_TAG)
@pytest.mark.parametrize("upload_file_str", ["upload_file_ndjson", "upload_file"])
def surrealdb_stager(
    request: TopRequest,
    upload_file_str: str,
    tmp_path: Path,
):
    upload_file: Path = request.getfixturevalue(upload_file_str)
    stager = SurrealDBUploadStager()
    stager_validation(
        configs=StagerValidationConfigs(test_id=CONNECTOR_TYPE, expected_count=22),
        input_file=upload_file,
        stager=stager,
        tmp_dir=tmp_path,
    )
